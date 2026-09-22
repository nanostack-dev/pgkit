# github.com/nanostack-dev/pgkit

PostgreSQL primitives for distributed systems in Go.

Requires Go 1.27 or newer (typed queue handles use generic methods).

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
