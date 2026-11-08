# 0003: Job states and leader-run maintenance

Status: Accepted

## Context

Jobs had four statuses (`pending`, `processing`, `done`, `failed`). `pending` mixed due, delayed and backing-off jobs; `failed` mixed exhausted, refused and (with a cancel feature) cancelled jobs. Every worker reaped its own queues on every replica, and retention ran on every replica. River, a mature PostgreSQL queue, solves the same problems with a seven-state model and maintenance services run by one elected leader. pgkit keeps its own queue (no River dependency) and adopts the design. The [lifecycle note](../technical/queue-job-lifecycle.md) has the comparison and the gaps by risk.

## Decision

- A `state` column holds `available`, `scheduled`, `running`, `retryable`, `completed`, `cancelled` or `discarded`. The legacy `status` column stays, kept in sync by a trigger in both directions, until a later contract migration removes it.
- Worker claims carry a lease (`lease_expires_at`) that `Heartbeat` extends.
- One client per database is the maintenance leader, elected through a lease row in `pgqueue_leader` with term identity and a local trust window. While it leads it runs the scheduler, the rescuer, each local worker's queue reap and, when configured, the retention cleaner. Workers join the election on their own.
- The claim also takes due `scheduled` and `retryable` jobs, so job progress never depends on the leader.
- `Cancel` and handler results `Cancel(err)` and `Snooze(d)` are typed outcomes; `NonRetryable` and `Fail` give `discarded`.

## Alternatives

- **Rename the status values in place.** A rolling deploy would break: old binaries claim `status = 'pending'`, and consumers such as echopoint's queue metrics read `status` with raw SQL.
- **Session advisory lock for leadership** (as `pgcron` does per schedule). It needs a pinned connection for the whole term and breaks under transaction pooling; a lease row is observable and pooler-safe.
- **Claim `available` jobs only** (River). It makes every delayed job wait for the leader and adds a failure mode when no client leads, while pgkit's claim (no priorities) is as cheap either way under the partial index.
- **Depend on River.** The features pgkit lacks are in the open-source core, but River Pro holds durable cron and workflows that pgkit already has; pgkit stays.

## Consequences

- Schema v5 runs online and is split by cost. `EnsureSchema` (start path) adds nullable columns and the trigger under a short, retried `lock_timeout` and backfills live jobs only. `CompleteSchema`, run by the maintenance leader or a migration job, backfills finished jobs, validates `NOT NULL` and builds the indexes concurrently. Each phase is serialized across replicas by its own advisory lock.
- `Job.State` is the field to use; `Job.Status` and the `Status*` constants remain as a legacy projection until a breaking release.
- `OnJobStuck` runs only in the leader process.
- The contract step (drop `status`, its indexes and the trigger) must be triggered by the operator once no binary older than schema v5 runs.
