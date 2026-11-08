# Queue job lifecycle and maintenance

This note compares the pgkit job model with [River](https://github.com/riverqueue/river)'s open-source core, ranks the gaps by incident risk, and records the design that closes them. River is a reference only: pgkit takes no dependency on it and copies none of its code (MPL-2.0). The decision is in [ADR 0003](../adr/0003-job-states-and-leader-maintenance.md).

## Comparison

| Concern | pgkit before schema v5 | River OSS | pgkit from schema v5 |
| --- | --- | --- | --- |
| States | `pending`, `processing`, `done`, `failed` | `available`, `scheduled`, `running`, `retryable`, `completed`, `cancelled`, `discarded`, `pending` | The seven River states except `pending`, in `state`; the old column `status` is kept in sync |
| Retries and backoff | `Retry` back to `pending` with a delay; exhausted jobs become `failed`. Exponential backoff with full jitter in the worker | `retryable` with `scheduled_at`; exhausted jobs become `discarded`. Retry policy on the client | `retryable` (or `available` for a zero delay); exhausted jobs become `discarded`. Backoff unchanged |
| Terminal failure | `NonRetryable` and `Fail` give `failed`, like exhaustion | `JobCancel` gives `cancelled`; exhaustion gives `discarded` | `NonRetryable` and `Fail` give `discarded`; a cancel request or `queue.Cancel(err)` gives `cancelled` |
| Stuck jobs | Each worker reaps its own queues on its own ticker, by `claimed_at` and its visibility timeout, on every replica | The leader's rescuer takes jobs running longer than `RescueAfter` (1 h), then checks the worker's timeout in its own process | Each worker claim stores a lease (`lease_expires_at`, extended by `Heartbeat`). The leader rescues expired leases in every queue, claims without a lease after `RescueAfter` (1 h), and runs each local worker's queue reap |
| Cleaning | `RunRetention` on every replica, `done` and `failed` only | Leader cleaner per final state (completed 24 h, cancelled and discarded 7 d by default) | Leader cleaner per final state when `MaintenanceConfig.Retention` is set; `RunRetention` still works. Nothing is deleted by default |
| Future jobs | Claim query filters `available_at <= NOW()` on `pending` | Leader scheduler moves due `scheduled` and `retryable` jobs to `available`; the claim takes `available` only | Leader scheduler moves due jobs to `available` and notifies their queues. The claim also takes due `scheduled` and `retryable` jobs, so no job waits on the leader |
| Leader election | None in `queue`; `pgcron` uses an advisory lock per schedule | Table lease with term identity `(leader_id, elected_at)`, local trust window, resignation notification | Same model, own implementation: table `pgqueue_leader`, renewal before a local trust deadline. Resignation deletes the term, and a follower takes over at its next election attempt (within `electEvery` plus jitter, 5 to 6 s); there is no resignation notification yet |
| Unique jobs | None | Unique key over kind, args, period and queue, enforced by a partial unique index over a state bitmask | Not yet; see [Left](#left) |
| Snooze | `SnoozeTx` refunds the attempt; zero time parks until `WakeTx` | `JobSnooze(d)` from a handler | `SnoozeTx` as before, plus `queue.Snooze(d)` returned by a handler. Both cancel a job that has a cancel request instead of snoozing it |
| Cancel | None | `JobCancel(err)` from a handler; `Client.JobCancel` for any job, running jobs through a notification to their producer | `queue.Cancel(err)` from a handler; `Client.Cancel` and `CancelTx`. A running job gets a cancel request: OnEnqueue workers cancel the handler's context, and the outcome is recorded as `cancelled` |

## Gaps by risk

1. **Liveness of maintenance (highest).** Reaping ran on every replica with each worker's own interval, so a queue whose workers all stopped never had its stuck jobs reaped, and N replicas ran N identical scans. A single elected leader with per-claim leases removes both problems, as long as the claim never depends on the leader to make progress.
2. **No cancel.** An operator could only delete a job, which loses its history and cannot stop a running handler. Cancel is a typed outcome now.
3. **Ambiguous `failed`.** Out of attempts, refused by the handler and (now) cancelled all looked the same. `discarded` and `cancelled` separate "gave up" from "asked to stop".
4. **Invisible delay.** `pending` covered due, delayed and backing-off jobs. Dashboards and the `queuestats` metrics in consumers had to repeat the claim filter to count waiting work. `available`, `scheduled` and `retryable` name these directly.
5. **Retention on every replica.** Harmless with `SKIP LOCKED` but wasteful; the leader cleaner runs it once and per final state.
6. **Duplicate enqueues.** Producers that retry after a timeout can enqueue twice. Unique jobs fix this and are the next step.

## Design

### State machine

```
            enqueue (due)                claim
  ┌───────────────────────► available ─────────► running ──► completed
  │                              ▲  ▲               │  │
  │ enqueue (later)              │  │ scheduler,    │  ├──► retryable ──► (scheduler) available
  └──────────► scheduled ────────┘  │ claim         │  ├──► discarded  (exhausted, NonRetryable, Fail)
                  ▲                 │               │  ├──► cancelled  (cancel request, queue.Cancel)
                  └── snooze ───────┴───────────────┘  └──► available  (rescue, hand back)
  Cancel: available | scheduled | retryable ──► cancelled; running ──► cancel requested
  Replay: completed | cancelled | discarded ──► available
```

`pending` is not used. In River it holds jobs that wait for an external action. pgkit expresses that as a job parked until `ParkedUntil`, and the word `pending` is the legacy value that means "claimable" in every binary still running the old code. Reusing it would let an old writer's row read as parked.

### Online migration (schema v5): expand now, complete in the background, contract later

Old binaries keep running while new ones start, and consumers (echopoint `queuestats`) read `status` with raw SQL. The migration therefore adds a column instead of renaming values. Apps run `EnsureSchema` on start, often under a deadline (fx: 15 s), so the migration is split by cost.

`EnsureSchema` (start path; work grows with live jobs, not with history):

1. Add the nullable columns `state`, `lease_expires_at` and `cancel_requested_at` (metadata only, no rewrite).
2. Install a trigger that keeps `state` and `status` in sync in both directions, and the state `CHECK` as `NOT VALID`. A new writer sets `state` and the trigger derives `status`; an old writer sets `status` and the trigger derives `state` (`pending` becomes `available`, `scheduled` or `retryable` from `available_at` and `attempts`, and a job parked at `ParkedUntil` is `scheduled`). The trigger also clears the lease whenever a job leaves `running`, so an old binary's claim never inherits a stale lease, and clears the cancel request when an old writer moves a finished job back to a live state (an old `ReplayJob`), as the new `ReplayJob` does.
3. Backfill `state` on `pending` and `processing` rows only, through the legacy partial indexes. Every job a claim, the scheduler or the rescuer can act on now has a state.

Steps 1 and 2 run only when the catalog shows them missing, each in a transaction with a 1 s `lock_timeout`. While DDL waits for its lock every writer queues behind it, so the wait stays short; on a timeout the transaction is retried with jittered backoff (up to 8 tries, while the caller's context lasts). A busy table delays the migration instead of stalling the queue.

`CompleteSchema` (background; reads the whole table, never blocks writers):

4. Build `idx_pgqueue_claimable`, `idx_pgqueue_due` and `idx_pgqueue_running` `CONCURRENTLY`, dropping an invalid leftover first.
5. Backfill `state` on the remaining (finished) rows in batches of 5000 without touching `updated_at`, then make it `NOT NULL` through a validated `CHECK` (no table scan under an exclusive lock), and validate the state `CHECK`.
6. Build `idx_pgqueue_finalized` `CONCURRENTLY`.

The maintenance leader runs `CompleteSchema` at the start of its term and retries every minute until it is done; operators can also call it from a migration job. It decides from the catalog, not the version marker, which `EnsureSchema` already set to 5. Each step is idempotent and kept when a run is interrupted, except an index build in progress, which the next run starts again (a lost term interrupts it, so a new leader rebuilds that index). On an idle database each index takes about 3 s per million rows and the backfill rewrites each finished row once. A table of at most 10,000 rows is completed inside `EnsureSchema`, with plain `CREATE INDEX`, so a new database is complete at once.

Until `CompleteSchema` has built the state indexes, the claim, the scheduler, the rescuer and the purge also filter on the equivalent `status` value, so the legacy partial indexes serve them. Finished jobs from before the migration read their state from `status` (`Job.State` is derived for them), but match no state filter, are not purged, and `ReplayJob` returns `ErrJobNotFound` for them until their row is backfilled.

`EnsureSchema` takes one session advisory lock and `CompleteSchema` another, so replicas that start together migrate once, and a replica never waits on an index build to start. The version marker only grows: an old binary's `EnsureSchema` skips every migration on a v5 database and then writes its own version back (`ON CONFLICT DO UPDATE SET value = EXCLUDED.value`), and a trigger on `pgqueue_meta` keeps the higher value. Without it, every later start would re-run migrations, and those before v5 take table locks even when they change nothing; while `CompleteSchema` builds an index, an `ALTER TABLE` or `CREATE INDEX` would queue behind the build and every writer behind it. Steps 1 and 2 also take no lock when present, for a replica that re-runs them while another migrates.

Mixed-fleet behavior: old workers still claim through `status = 'pending'`, ack through `status = 'processing'`, reap their own claims and wake parked jobs. New workers claim through `state`. Both see every job. A new claim clears any cancel request left from an earlier life of the job; the same clearing drops a request on a running job that an old binary snoozed or retried. An old binary ignores cancel requests on the jobs it runs: its ack records `completed`, its retry `retryable` or `available`. Cancel requests reach only new workers. Old binaries' `ReplayJob` and `RunRetention` keep working through the synced `status`; they treat cancelled jobs as failed. A job an old binary snoozed (not parked) to a future time with attempts left reads as `retryable` rather than `scheduled`: the trigger cannot tell a snooze from a retry.

The contract step drops `status`, its indexes and the trigger once no old binary is left, and with them the redundant `status` filters. It cannot be automatic, because `EnsureSchema` cannot know when the fleet is upgraded. It is listed under [Left](#left).

### Leader election

`pgqueue_leader` holds one row per leadership name. A client wins when the row is missing or expired, renews by `(leader_id, elected_at)`, and resigns by deleting its own term. Followers learn of a resignation only at their next election attempt, within `electEvery` plus jitter; a resignation notification would shorten that and is listed under [Left](#left). A leader trusts its term until `attempt start + TTL − margin` by its local monotonic clock; if renewal keeps failing past that, it stops its services before the database lease can expire. Defaults: elect every 5 s, TTL 15 s, renew every 5 s. A table lease works through transaction poolers and needs no pinned connection, unlike a session advisory lock. A short overlap of two leaders is harmless: every service locks its rows with `SKIP LOCKED` in a scan whose condition PostgreSQL rechecks against the newest row version, and its update repeats the state guard. A condition behind a subquery would not be rechecked: the rescuer once revived jobs settled between its snapshot and its row lock that way.

### Maintenance services

Every `Worker.Run` joins its client's maintainer; `Client.RunMaintenance` joins without a worker. The first to join starts the elector and the last to leave resigns. While it leads, the maintainer runs:

- **Scheduler** (every 1 s): moves due `scheduled` and `retryable` jobs to `available` in batches and notifies each queue, so OnEnqueue workers take delayed jobs and retries as they fall due instead of at the next rescan.
- **Rescuer** (every 30 s): a running job whose lease expired, or without a lease and claimed before `RescueAfter`, becomes `available` (attempts left), `discarded` (exhausted) or `cancelled` (cancel requested). Each local worker also reaps its own queues by its visibility timeout and reap interval, as before, but only on the leader. `OnJobStuck` therefore runs in the leader process.
- **Schema completer** (at the start of each term, then every minute until done): runs `CompleteSchema` unless the catalog shows schema v5 complete or another client holds its lock.
- **Cleaner** (every `Retention.Interval`, only with `MaintenanceConfig.Retention`): deletes completed, discarded and cancelled jobs older than their retention, in batches.

## Left

- **Unique jobs.** Planned shape: `EnqueueParams.Unique` with `ByPeriod` and `States`; a `unique_key` (SHA-256 of queue name, canonical payload and period bucket) and a `unique_states` bitmask; a partial unique index `WHERE unique_key IS NOT NULL AND pgqueue_state_in(unique_states, state)`; `INSERT … ON CONFLICT DO NOTHING` returning the existing job. Transitions that re-enter the index (scheduler, replay) must handle the conflict, for example by discarding the duplicate.
- **Contract migration** dropping `status` and the trigger, behind an explicit call after the fleet is upgraded, followed by a breaking release that removes `Job.Status` and the `Status*` constants.
- **Admin UI** still groups jobs by the legacy status; it should show the seven states, cancel, and the current leader.
- **`fx/queue`** `Options.Retention` still starts `RunRetention` on every replica; it should set `MaintenanceConfig.Retention` instead.
- **Legacy filters stay coarse:** `Purge(PurgeParams{Status: StatusFailed})` and `ListJobsParams{Status: StatusFailed}` cover both discarded and cancelled jobs.
- **Workflow orphan detection** still reads `pgqueue_jobs.status` (kept in sync); it should read `state` before the contract step.
- **Resignation notification** on `pgqueue.leader`, so a follower takes over at once instead of at its next election attempt.
- **Stuck-handler diagnostics** (a goroutine dump when a handler ignores cancellation) and a notify limiter for bursts.
- **Driver conformance suite** for the `db` stage: the new tests run on pgx and lib/pq handles, the older queue tests on pgx only.
