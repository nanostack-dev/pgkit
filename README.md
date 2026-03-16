# github.com/nanostack-dev/pgkit

PostgreSQL primitives for distributed systems in Go.

Install:

```bash
go get github.com/nanostack-dev/pgkit
```

## Packages

- `pglock`: advisory lock helpers (`transaction` and `session` scoped)
- `queue`: durable queue with claim/ack/retry/fail/reap
- `workflow`: durable temporal-style workflows built on top of `queue`
- `adminui`: an embedded dashboard (SvelteKit + Skeleton) to monitor queues and workflows
- `fx`: plug-and-play Uber Fx modules for all packages

## Worker runtime

`queue` includes a worker runtime with queue listeners:

- raw listener: `registry.Register("queue", func(ctx, job){ ... })`
- typed JSON listener with metadata: `RegisterJSONTyped[T](...)`
- typed JSON listener only: `RegisterJSON[T](...)`

Worker handles claim loop, ack/retry/fail, and stuck-job reaping.

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
