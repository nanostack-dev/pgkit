# github.com/nanostack-dev/pgkit

PostgreSQL primitives for distributed systems in Go.

Requires Go 1.27 or newer (typed queue handles and workflow operations use generic methods).

Standalone contributor setup, architecture, tests and release procedures: [docs/README.md](docs/README.md). Agent rules: [AGENTS.md](AGENTS.md); canonical vocabulary: [CONTEXT.md](CONTEXT.md).

Install:

```bash
go get github.com/nanostack-dev/pgkit
```

## Packages

- `pglock`: advisory lock helpers (`transaction` and `session` scoped)
- `pgcron`: durable interval schedules shared across replicas ([docs](pgcron/README.md))
- `queue`: durable queue with claim/ack/retry/fail/reap
- `workflow`: durable Go functions with typed, checkpointed steps, sleeps, signals and child runs, built on `queue` ([docs](workflow/README.md))
- `adminui`: an embedded admin panel (React) to monitor and operate queues, workflow runs and advisory locks
- `fx`: Uber Fx modules for locks, queues, workflows, and the dashboard

## Worker runtime

`queue` includes a worker runtime with queue listeners:

- raw listener: `registry.Register("queue", func(ctx, job){ ... })`
- typed JSON listener with metadata: `RegisterJSONTyped[T](...)`
- typed JSON listener only: `RegisterJSON[T](...)`

Worker handles claim loop, ack/retry/fail, cancel and snooze, and takes part in
the election of the maintenance leader (see [Job states and maintenance](#job-states-and-maintenance)).

Bind a queue name and JSON payload type once for both producers and workers:

```go
type Email struct {
    To string `json:"to"`
}

emails := client.Queue[Email]("emails").WithOptions(queue.EnqueueOptions{
    MaxAttempts: 5,
})
err := emails.Register(registry, func(ctx context.Context, payload Email, job queue.Job) error {
    return sendEmail(ctx, payload.To)
})
// Check err before starting the worker.
id, err := emails.Enqueue(ctx, Email{To: "recipient@example.com"})
// Or enqueue atomically with other writes: emails.EnqueueTx(ctx, tx, payload).
```

`WithOptions` returns a copy, so per-call overrides do not mutate a shared
handle. `AvailableAt` defaults to database time; `MaxAttempts` defaults to 5.
The handle validates the queue name on enqueue/registration, and reports JSON
encoding errors before writing. Malformed stored JSON fails without retry;
handlers remain responsible for business-field validation. Payload types are
not registered in PostgreSQL: all callers using a queue name must agree on a
compatible JSON shape, including old producers during rolling deployments.
Raw enqueue and `RegisterJSON`/`RegisterJSONTyped` remain supported.

### Picking up jobs

Build a worker for one or more typed queues; `Pickup` decides when it looks for
jobs:

```go
worker, err := client.Worker("notifications").
    Pickup(queue.OnEnqueue()). // claim at commit, rescan every 5s
    Handle(emails, sendEmail).
    Handle(receipts, sendReceipt).
    Tune(queue.WorkerConfig{VisibilityTimeout: time.Minute}).
    Build()

queue.OnEnqueue().RescanEvery(30 * time.Second)
queue.PollEvery(250 * time.Millisecond) // scan only, no LISTEN
```

`Handle` is generic over the queue's payload type; `HandleRaw` takes a queue name
and an undecoded `Handler`. Builder methods return copies, so a shared base can
be extended without affecting other workers. `NewWorker` with a `HandlerRegistry`
and `WorkerConfig.Pickup` remains available.

With `OnEnqueue`, enqueue, replay and the reaper send a `pg_notify` on
`queue.NotifyChannel`, delivered when their transaction commits. Every worker of
every replica listening for that queue wakes; `FOR UPDATE SKIP LOCKED` splits the
jobs between them. Identical notifications in one transaction collapse into one,
and a worker coalesces notifications that arrive while it is busy into a single
extra scan. A batch that fills up is followed by another scan straight away.

Notifications are hints, so the worker still rescans: delayed jobs and retries
are picked up within one rescan interval of falling due, and the listener scans
once each time it (re)connects to recover anything sent while it was down. All
workers of a `Client` share one `LISTEN` connection, opened outside the `*sql.DB`
pool. With the pgx stdlib driver it is configured like the pool's connections.
With any other driver (lib/pq, instrumentation wrappers), give it a connection
before building workers, or `Build` returns `ErrNotificationsUnsupported`:

```go
err := client.ListenOn(postgresConfig.DSN())            // URL or key=value DSN
client.ListenWith(func(ctx context.Context) (*pgx.Conn, error) {
    return pgx.ConnectConfig(ctx, freshConfigWithRotatedPassword())
})
```

`Worker.Ready()` closes after the first scan in which every claim succeeded with
the listener up; from then on a newly claimable job wakes the worker.

`PollInterval` still works and means `PollEvery(PollInterval)`; set it or
`Pickup`, not both.

Each worker reaps stuck jobs of its own queues with its `VisibilityTimeout`, but
only in the process that is the maintenance leader (see
[Job states and maintenance](#job-states-and-maintenance));
`client.Reap(ctx, queue.ReapParams{...})` does the same for chosen queues and
`ReapStuckJobs` for all of them, from any process. Workers claim and settle jobs on contexts that
shutdown does not cancel: a handler finishing while its worker stops still settles
its job, and a job claimed as the worker stops is handed back at once.

### Job states and maintenance

A job is `available` (due), `scheduled` (enqueued for later, snoozed or parked),
`running` (claimed), `retryable` (waiting out its backoff), `completed`,
`cancelled` (asked to stop) or `discarded` (attempts exhausted, `NonRetryable` or
`Fail`). `Job.State` and `ListJobsParams.State` use these. `Job.Status` and the
`pgqueue_jobs.status` column keep the older coarse values (`pending`, `processing`,
`done`, `failed`) in sync for existing code and queries; prefer the state.

The claim takes available jobs and scheduled or retryable jobs that are due, so
no job waits on maintenance. One client per database is elected leader
(`pgqueue_leader`, renewed every 5 s, 15 s lease); every running worker takes
part, and `client.RunMaintenance(ctx)` takes part without a worker. While it
leads, a client:

- makes due scheduled and retryable jobs available every second and notifies
  their queues, so OnEnqueue workers take delayed jobs and retries as they fall due;
- takes back running jobs whose lease expired (worker claims carry a lease of
  `VisibilityTimeout`, which `Heartbeat` extends), and claims without a lease
  (`Client.Claim`, binaries older than schema v5) after `RescueAfter` (one hour);
- runs each local worker's reap of its own queues every `ReapInterval`, so
  `OnJobStuck` runs in the leader process only;
- purges finalized jobs when `MaintenanceConfig.Retention` is set;
- finishes the schema migration with `CompleteSchema` until it is complete.

```go
err := client.SetMaintenance(queue.MaintenanceConfig{
	Retention: &queue.RetentionPolicy{KeepDone: 24 * time.Hour, KeepFailed: 7 * 24 * time.Hour, KeepCancelled: 7 * 24 * time.Hour},
})
leader := client.IsLeader()
```

A handler, or anyone, can stop or delay a job:

```go
state, err := client.Cancel(ctx, jobID)  // or CancelTx; cancelled now, or a cancel request when running
return queue.Cancel(err)                // from a handler: cancelled, not retried, no OnJobFailed
return queue.Snooze(10 * time.Minute)   // from a handler: scheduled again, attempt not counted
```

A cancel request on a running job cancels the handler's context with cause
`queue.ErrJobCancelled` on OnEnqueue workers; on polling workers the handler runs
to its end. Either way the job becomes cancelled unless the handler succeeds.
The [lifecycle note](docs/technical/queue-job-lifecycle.md) has the design and the
online migration.

### Long-running and parked jobs

`OnJobFailed` runs after a job is discarded (a terminal failure or an exhausted
retry) and that has been persisted successfully. Its job snapshot has
`StateDiscarded`, `StatusFailed` and the handler error in `LastError`; without a
callback, settlement leaves that in-memory snapshot untouched.

These primitives let a handler keep, park or hand back the job it holds. The
workflow package is built on them.

```go
err := client.Heartbeat(ctx, job)                  // extend the claim; ErrJobNotFound once it is lost
err = client.LockClaimTx(ctx, tx, job)             // hold the claim (row lock) for the rest of tx
err = client.SnoozeTx(ctx, tx, job, until)         // scheduled until until, attempt not counted
err = client.SnoozeTx(ctx, tx, job, time.Time{})   // parked until woken (available at queue.ParkedUntil)
// A job with a cancel request becomes cancelled instead of snoozed or parked.
err = client.WakeTx(ctx, tx, jobID)                // a parked or delayed job becomes due now, workers notified
```

`Heartbeat`, `LockClaimTx` and `SnoozeTx` act only on the claim described by
`job`, its ID and claim number (`Job.Claims`), so a handler whose job was reaped,
or failed and replayed, and claimed elsewhere cannot touch the new claim. The claim
number counts every claim and only grows; `attempts` stays the retry budget, which
`SnoozeTx` refunds and a replay resets. Durations and timeouts keep sub-second
precision; `Reap` refuses a non-positive visibility timeout. `EnsureSchema` adds
the `claims` column to existing queues (schema version 2), the `trace_context`
column (schema version 3, see [Tracing](#tracing)), the finished-jobs index
(schema version 4, see [Retention](#retention)) and job states with leases,
cancel requests and the leader table (schema version 5).

`EnsureSchema` is safe on the start path of a large, busy queue. It takes table
locks with a 1 s `lock_timeout` and retries with backoff, so writers never wait
more than a second behind it, and it backfills the state of live jobs only. The
steps of schema version 5 that read the whole table (backfilling the state of
finished jobs, validating constraints, building the state indexes) run in
`client.CompleteSchema(ctx)`. The maintenance leader calls it on its own, so a
deployment with a running worker or `RunMaintenance` needs nothing more; call it
from a migration job to finish at a time of your choice. Expect about 3 s per
index per million rows (four indexes), plus the backfill of finished jobs; it
never blocks writers. On a table of at most 10,000 rows, a new database included,
`EnsureSchema` completes the schema itself. Until `CompleteSchema` finishes,
finished jobs from before the migration match no state filter, are not purged,
and `ReplayJob` returns `ErrJobNotFound` for them.

`client.Subscribe(keys...)` and `client.NotifyTx(ctx, tx, key)` reuse the shared
`LISTEN` connection for other keys: a subscription wakes when a notification for
one of its keys commits, and when the listener reconnects.

### Retention

Completed, discarded and cancelled jobs stay in `pgqueue_jobs` until something
deletes them. Without
retention the table grows with every job ever run, and each `ListJobs`, `CountJobs`
and dashboard read gets slower. `RunRetention` deletes them on a schedule, for every
queue in the database:

```go
go client.RunRetention(ctx, queue.RetentionPolicy{
	KeepDone:      7 * 24 * time.Hour,  // completed
	KeepFailed:    30 * 24 * time.Hour, // discarded
	KeepCancelled: 30 * 24 * time.Hour,
})
```

`MaintenanceConfig.Retention` runs the same purge on the elected leader only,
instead of on every replica.

With `fx/queue`, set `queuefx.Options{Retention: &policy}` instead; the app then
fails to start on an invalid policy, and the loop stops with the app.

- Age is measured from the job's last update (`updated_at`), so a job is kept for
  its full period after it finishes, however long it waited before.
- A zero `KeepDone`, `KeepFailed` or `KeepCancelled` keeps that state forever; the
  policy must set at least one. Jobs that are not finalized are never deleted.
- It purges at start and then every `Interval` (default one hour), in batches of
  `BatchSize` jobs (default 1000), each batch its own transaction. The first run on
  a large table therefore deletes its backlog without one long lock.
- Batches select with `FOR UPDATE SKIP LOCKED`, so every replica may run it. A
  failed purge is logged and retried at the next interval.
- `PurgeFinished` runs one purge and returns the counts. `Purge` still deletes one
  status at a time on demand.

The purge queries name the state literally, so they use the partial index
`idx_pgqueue_finalized (state, updated_at) WHERE state IN ('completed', 'cancelled', 'discarded')`
under any plan, generic plans included.

### Tracing

The queue propagates OpenTelemetry trace context through the job row, so a job
enqueued while handling a request runs in that request's distributed trace. pgkit
uses only the OpenTelemetry API and the global `otel.GetTracerProvider()` and
`otel.GetTextMapPropagator()`; the application installs the SDK and exporter.

```go
otel.SetTracerProvider(sdkProvider)
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
```

- **Enqueue:** when `ctx` carries a valid span context, `Enqueue`/`EnqueueTx` inject
  it with the global propagator (W3C `traceparent`/`tracestate`, plus anything else
  the propagator writes) and store the carrier in the nullable `trace_context JSONB`
  column. Without a span or a propagator the column stays `NULL`.
- **Execute:** each claim the worker runs is a `SpanKindConsumer` span named
  `pgkit.queue <queue name>`, a child of the stored context (a new root when it is
  `NULL`). The handler's `ctx` carries it, so database calls, HTTP calls and log lines
  that read the span from `ctx` belong to the trace; settling the outcome (ack,
  retry, fail) runs under it too.
- **Attributes:** `messaging.system=pgkit`, `messaging.operation.type=process`,
  `messaging.destination.name` (queue), `messaging.message.id` (job ID),
  `pgkit.queue.job.attempt`, `pgkit.queue.job.max_attempts` and
  `pgkit.queue.job.outcome` (`done`, `retry`, `failed` or `handled`).
- **Errors:** a handler error is recorded on the span with status `Error`; the
  outcome tells a retry from a terminal failure (non-retryable or attempts
  exhausted). `Handled` leaves the status unset.

Every attempt, a replay included, is a child of the enqueuer's span rather than a
new trace linked to it: the job's whole history stays in one trace, each attempt
told apart by its attempt attribute, at the cost of a trace that spans the retry
delays. Workflow runs go through the same path: each activation of a run is a
consumer span in the trace of the `Start` call, and child runs, started from an
activation, join it too.

With no TracerProvider configured the global no-op tracer costs a few small
allocations per job, negligible beside the claim's database round trip, and the
default no-op propagator stores nothing. Install a propagator, or nothing is
stored even when spans are recorded.

## Admin UI Dashboard

The admin panel is fully embedded (React static build + JSON API) and protected with Basic Auth. It covers queue health and jobs, workflow runs with their checkpoint timeline, run trees and advisory locks, works on phones, and supports light and dark themes. See [docs/technical/admin-ui.md](docs/technical/admin-ui.md).

Required env var:

- `PGKIT_DASHBOARD_TOKEN`: password used in Basic Auth

Optional env vars:

- `PGKIT_DASHBOARD_ENABLE_API` (`true` by default): enables/disables mutating endpoints

## Running the Playground

The easiest way to try pgkit (queue, workflows, admin UI) is the playground. It
starts PostgreSQL with testcontainers, applies the schemas, runs order-fulfilment
workflows that succeed, retry, wait for approval, fail and get cancelled, and serves
the admin UI.

```bash
PGKIT_DASHBOARD_TOKEN="change-me" go run ./cmd/pgkit-playground
```

Open `http://127.0.0.1:18081` (or `PGKIT_PLAYGROUND_ADDR`) and authenticate with any
username and `PGKIT_DASHBOARD_TOKEN` as password.

## Custom logger adapter

`pglock.Client`, `queue.Client`, and `workflow.Client` all support custom logging adapters:

```go
type Logger interface {
    Debug(ctx context.Context, msg string, fields map[string]any)
    Info(ctx context.Context, msg string, fields map[string]any)
    Warn(ctx context.Context, msg string, fields map[string]any)
    Error(ctx context.Context, msg string, fields map[string]any)
}
```

Use `SetLogger(...)` to plug your own logger.
