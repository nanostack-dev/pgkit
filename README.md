# github.com/nanostack-dev/pgkit

PostgreSQL primitives for distributed systems in Go.

Requires Go 1.27 or newer (typed queue handles use generic methods).

Standalone contributor setup, architecture, tests and release procedures: [docs/README.md](docs/README.md). Agent rules: [AGENTS.md](AGENTS.md); canonical vocabulary: [CONTEXT.md](CONTEXT.md).

Install:

```bash
go get github.com/nanostack-dev/pgkit
```

## Packages

- `pglock`: advisory lock helpers (`transaction` and `session` scoped)
- `pgcron`: durable interval schedules shared across replicas ([docs](pgcron/README.md))
- `queue`: durable queue with claim/ack/retry/fail/reap
- `workflow`: durable temporal-style workflows built on top of `queue` ([docs](workflow/README.md))
- `adminui`: an embedded dashboard (SvelteKit + Skeleton) to monitor queues and workflows
- `fx`: Uber Fx modules for locks, queues, workflows, and the dashboard

## Worker runtime

`queue` includes a worker runtime with queue listeners:

- raw listener: `registry.Register("queue", func(ctx, job){ ... })`
- typed JSON listener with metadata: `RegisterJSONTyped[T](...)`
- typed JSON listener only: `RegisterJSON[T](...)`

Worker handles claim loop, ack/retry/fail, and stuck-job reaping.

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

## Admin UI Dashboard

The dashboard is fully embedded (SvelteKit static build + JSON API) and protected with Basic Auth.

Required env var:

- `PGKIT_DASHBOARD_TOKEN`: password used in Basic Auth

Optional env vars:

- `PGKIT_DASHBOARD_ENABLE_API` (`true` by default): enables/disables mutating endpoints

## Running the Playground

The easiest way to test out the full pgkit suite (Queue, Workflows, Admin UI) is to run the playground.
It uses testcontainers to automatically spin up a local PostgreSQL instance, applies all schemas, runs sample background tasks, and starts the Admin UI server.

```bash
cd pgkit
PGKIT_DASHBOARD_TOKEN="change-me" go run ./cmd/pgkit-playground
```

Open `http://localhost:8080` and authenticate with any username + `PGKIT_DASHBOARD_TOKEN` as password.

## Custom logger adapter

`pglock.Client`, `queue.Client`, and `workflow.Module` all support custom logging adapters:

```go
type Logger interface {
    Debug(ctx context.Context, msg string, fields map[string]any)
    Info(ctx context.Context, msg string, fields map[string]any)
    Warn(ctx context.Context, msg string, fields map[string]any)
    Error(ctx context.Context, msg string, fields map[string]any)
}
```

Use `SetLogger(...)` to plug your own logger.
