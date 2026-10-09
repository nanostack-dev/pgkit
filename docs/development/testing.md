# Testing

Run commands from the Go module root. [CI](../../.github/workflows/ci.yml) runs the configured golangci-lint plus the full race-enabled suite with a ten-minute timeout.

```sh
golangci-lint run
go test -race -count=1 -timeout 10m ./...
go build ./...
```

Select affected packages for iteration: `go test -race -count=1 ./queue ./workflow`, `./pglock`, `./pgcron` or `./adminui`. The lock, queue, scheduling, workflow and admin tests use real PostgreSQL through testcontainers; fake database tests are insufficient evidence for locking and concurrent state changes.

For frontend changes, run the commands in [setup](setup.md), regenerate embedded assets, then test `./adminui` and the playground. Verify dashboard auth, mutation disabling and CSRF when touching those paths. Queue changes must cover transitions, retries, claim exclusivity, claim fencing and restart recovery.

## Workflow suite

The workflow tests share one PostgreSQL container per test binary and clone a fresh database from a migrated template for each test ([main_test.go](../../workflow/main_test.go)), so tests run in parallel and notifications stay isolated. Helpers start workers, await results, dump a stuck run's state on failure and fast-forward parked runs past their sleeps and timeouts.

- One test: `go test -race -count=1 -run TestReceiveTimesOut ./workflow`.
- `TestManyRunsUnderContention` drives every operation on competing workers and stops one mid-flight; `-short` skips it. After runtime changes, repeat it: `go test -race -count=20 -run ManyRuns ./workflow`.
- `TestEveryOperationWorksOnLibPQ` runs a full scenario on lib/pq; keep SQL parameters driver-neutral (no array parameters, JSONB passed as text).
- Workflow changes must keep checkpoints compatible with stored runs, the fencing of every write, and the park re-check that prevents lost wakeups.

Timing-sensitive tests keep margins of several heartbeats: on a loaded machine a sub-second visibility timeout lets the reaper take over a run whose heartbeats are merely late.

Documentation-only changes need local link checks and `git diff --check`; they do not require creating runtime tests. Report skipped database checks separately from passing ones.
