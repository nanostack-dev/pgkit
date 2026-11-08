# Testing

Run commands from the Go module root. [CI](../../.github/workflows/ci.yml) runs the configured golangci-lint plus the full race-enabled suite with a ten-minute timeout.

```sh
golangci-lint run
go test -race -count=1 -timeout 10m ./...
go build ./...
```

Select affected packages for iteration: `go test -race -count=1 ./queue ./workflow`, `./pglock`, `./pgcron` or `./adminui`. The lock, queue, scheduling, workflow and admin tests use real PostgreSQL through testcontainers; fake database tests are insufficient evidence for locking and concurrent state changes.

For frontend changes, run `check`, `lint` and `build` from [setup](setup.md), regenerate embedded assets, then test `./adminui` and the playground. Review the changed screens with the worst-case fixtures (`?data=worst` in `pnpm dev`) and against the playground at phone (390 px), tablet (820 px) and desktop widths in light and dark themes, including keyboard navigation and replay, delete, enqueue, retry and cancel. Verify dashboard auth, mutation disabling and CSRF when touching those paths; API changes need `./adminui` tests against PostgreSQL. Queue changes must cover transitions, retries, claim exclusivity, claim fencing and restart recovery. Tests of job states, maintenance, leader election and cancel use `eachDriver` ([drivers_test.go](../../queue/drivers_test.go)): each runs on pgx and lib/pq handles, on its own database in the shared container, because maintenance acts on every queue. Elector timing is tested without a database in `testing/synctest` bubbles with a fake leader store. Schema migrations must be tested from a schema built by the previous version (`migrateToV4`), with old-binary writers running alongside, with more rows than one backfill batch, under lock contention and interrupted midway; the schema tests lower `Client.schema` thresholds instead of building large tables. [compat_test.go](../../queue/compat_test.go) builds the released queue that consumers deploy ([testdata/oldpgkit](../../queue/testdata/oldpgkit), its own module pinned to that release) and runs it as a separate process next to the current code, on a schema after `EnsureSchema` and after `CompleteSchema`; bump the pinned version there when consumers move to a newer release that a schema change must stay compatible with. A race between a settle and a maintenance pass is forced with `pg_sleep` in the pass's condition rather than left to timing.

## Workflow suite

The workflow tests share one PostgreSQL container per test binary and clone a fresh database from a migrated template for each test ([main_test.go](../../workflow/main_test.go)), so tests run in parallel and notifications stay isolated. Helpers start workers, await results, dump a stuck run's state on failure and fast-forward parked runs past their sleeps and timeouts.

- One test: `go test -race -count=1 -run TestReceiveTimesOut ./workflow`.
- `TestManyRunsUnderContention` drives every operation on competing workers and stops one mid-flight; `-short` skips it. After runtime changes, repeat it: `go test -race -count=20 -run ManyRuns ./workflow`.
- `TestEveryOperationWorksOnLibPQ` runs a full scenario on lib/pq; keep SQL parameters driver-neutral (no array parameters, JSONB passed as text).
- Workflow changes must keep checkpoints compatible with stored runs, the fencing of every write, and the park re-check that prevents lost wakeups.

Timing-sensitive tests keep margins of several heartbeats: on a loaded machine a sub-second visibility timeout lets the reaper take over a run whose heartbeats are merely late.

Documentation-only changes need local link checks and `git diff --check`; they do not require creating runtime tests. Report skipped database checks separately from passing ones.

`internal/testdb.Open` owns disposable PostgreSQL containers for lock, queue,
worker and admin API tests. It keeps each caller's database name, waits for
PostgreSQL readiness, and registers pool/container cleanup even when a later
setup step fails. Startup honors the caller context; cleanup has its own bounded
context so cancellation cannot leak the container. Workflow tests keep their
specialized shared server and migrated template cloning for parallel isolation.
