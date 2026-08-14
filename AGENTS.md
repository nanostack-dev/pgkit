# pgkit Agent Guide

Go PostgreSQL primitives: `pglock` (advisory locks), `pgqueue` (durable queue), optional dashboard/API.

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
