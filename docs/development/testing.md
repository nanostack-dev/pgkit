# Testing

Run commands from the Go module root. [CI](../../.github/workflows/ci.yml) runs the configured golangci-lint plus the full race-enabled suite with a ten-minute timeout.

```sh
golangci-lint run
go test -race -count=1 -timeout 10m ./...
go build ./...
```

Select affected packages for iteration: `go test -race -count=1 ./queue ./workflow`, `./pglock`, `./pgcron` or `./adminui`. The lock, queue, scheduling, workflow and admin tests use real PostgreSQL through testcontainers; fake database tests are insufficient evidence for locking and concurrent state changes.

For frontend changes, run the commands in [setup](setup.md), regenerate embedded assets, then test `./adminui` and the playground. Verify dashboard auth, mutation disabling and CSRF when touching those paths. Queue changes must cover transitions, retries, claim exclusivity and restart recovery. Workflow changes must preserve foreach dependency readiness and stored-version compatibility.

Documentation-only changes need local link checks and `git diff --check`; they do not require creating runtime tests. Report skipped database checks separately from passing ones.
