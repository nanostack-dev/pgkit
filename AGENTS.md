# pgkit Agent Guide

Go PostgreSQL primitives: `pglock` (advisory locks), `queue` (durable jobs), `pgcron` (durable interval schedules), `workflow` and optional dashboard/API.

## Read by task

This repository is sufficient for standalone work. A shared Nanostack workspace or installed skills may help, but no parent checkout or workspace bootstrap is required.

- Domain terms or payload contracts: read [CONTEXT.md](CONTEXT.md).
- Package boundaries or runtime behavior: read [architecture](docs/technical/architecture.md), then the affected package docs linked from [the index](docs/README.md).
- Local startup or missing prerequisites: read [setup](docs/development/setup.md) and [troubleshooting](docs/development/troubleshooting.md).
- Code changes: select the checks in [testing](docs/development/testing.md).
- Publishing or changing a consumer's dependency: read [deployment](docs/runbooks/deployment.md); a failed upgrade follows [rollback](docs/runbooks/rollback.md).

## Invariants

- Prefer PostgreSQL-native primitives: advisory locks, `FOR UPDATE SKIP LOCKED`, DB time via `NOW()`.
- Queue transitions are explicit and guarded, especially `processing -> done|pending|failed`.
- Public APIs stay stable; extend via params structs rather than new positional args.
- Never surface internal DB errors, secrets, or auth tokens through the dashboard/API.
- Mutating dashboard/API endpoints stay disable-able in production and are protected by constant-time token auth plus CSRF mitigation.
- App-specific workflow naming belongs in the calling app, not here.
- Avoid comments — name variables and functions clearly instead. Comment only a genuinely complex algorithm.

## Verification

Lock/queue behavior needs real PostgreSQL (testcontainers), not a fake. Use `-race` for concurrency-sensitive changes.

## Delivery and documentation

Use an isolated worktree based on fetched `origin/main`; preserve the primary checkout. Use Conventional Commits and open a focused PR after relevant local checks. Report the exact checks and any skipped database verification; CI is the final verification gate. A review request ends with findings and a verdict before implementation.

When behavior, an invariant, a domain term or a verified recurring fix changes, update its owning documentation in the same PR. Current behavior belongs in `docs/technical/` or the existing package README, development fixes in `docs/development/`, and operational procedures in `docs/runbooks/`. Add an ADR only for a consequential, hard-to-reverse choice with real alternatives; preserve existing decision numbers and supersede reversals with a new record. Keep [docs/README.md](docs/README.md) current. `AGENTS.md` is the sole agent guide filename.
