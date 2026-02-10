# nanostack.dev/pgkit

PostgreSQL primitives for distributed systems in Go.

## Packages

- `pglock`: advisory lock helpers (`transaction` and `session` scoped)
- `pgqueue`: durable queue with claim/ack/retry/fail/reap and an embedded dashboard

## Worker runtime

`pgqueue` includes a worker runtime with queue listeners:

- raw listener: `registry.Register("queue", func(ctx, job){ ... })`
- typed JSON listener with metadata: `RegisterJSONTyped[T](...)`
- typed JSON listener only: `RegisterJSON[T](...)`

Worker handles claim loop, ack/retry/fail, and stuck-job reaping.

## pgqueue dashboard

The dashboard is embedded (HTMX + Tailwind template) and protected with Basic Auth.

Required env var:

- `PGKIT_DASHBOARD_TOKEN`: password used in Basic Auth

Optional env vars:

- `PGKIT_DASHBOARD_ENABLE_API` (`true` by default): enables/disables enqueue endpoints (`/enqueue` and `/api/jobs`)

## Example app

Run the embedded queue dashboard example:

```bash
PGKIT_DATABASE_URL="postgres://pgkit:pgkit@localhost:5432/pgkit_test?sslmode=disable" \
PGKIT_DASHBOARD_TOKEN="change-me" \
go run ./cmd/pgkit-example
```

Open `http://localhost:8080` and authenticate with any username + `PGKIT_DASHBOARD_TOKEN` as password.

## Custom logger adapter

Both `pglock.Client` and `pgqueue.Client` support custom logging adapters:

```go
type Logger interface {
    Debug(ctx context.Context, msg string, fields map[string]any)
    Info(ctx context.Context, msg string, fields map[string]any)
    Warn(ctx context.Context, msg string, fields map[string]any)
    Error(ctx context.Context, msg string, fields map[string]any)
}
```

Use `SetLogger(...)` to plug your own logger.
