# Agent Guide: pgkit

`pgkit` provides PostgreSQL-native primitives for distributed Go services.

## Key Responsibilities
- Provide safe advisory lock helpers in `pglock`.
- Provide a durable queue implementation in `pgqueue`.
- Keep the embedded dashboard secure and optional for production.

## Tech Stack
- **Language**: Go
- **Database**: PostgreSQL
- **Testing**: testcontainers-go with real PostgreSQL

## Best Practices
- Prefer PostgreSQL-native primitives (`FOR UPDATE SKIP LOCKED`, advisory locks).
- Keep queue state transitions explicit and guarded (`processing -> done|pending|failed`).
- Use database time (`NOW()`) for scheduling-sensitive operations.
- Keep public APIs stable; prefer params structs for extensibility.
- Never expose internal DB errors in HTTP responses.
- Keep mutating dashboard/API endpoints disable-able in production.

## Security Guidelines
- Protect dashboard/API endpoints with token-based auth via env config.
- Use constant-time token comparisons.
- Apply CSRF mitigation on mutating endpoints.
- Do not log secrets or auth tokens.

## Logging & Observability
- All packages should support pluggable logging adapters.
- Emit structured operation events (enqueue, claim, retry, fail, reap, purge).
- Provide hooks for metrics/instrumentation without hard-coding vendors.

## Testing Standards
- Concurrency and lock behavior must be validated with race-enabled tests.
- Queue tests should cover:
  - duplicate claim prevention
  - stuck job reaping
  - retry exhaustion behavior
  - state guard failures
- Integration-style tests should run against a real PostgreSQL container.

## Git Conventions
- **Commit Messages**: Follow [Conventional Commits](https://www.conventionalcommits.org/)
  - `feat: add queue stuck-job reaper`
  - `fix: enforce processing-state guard in retry`
  - `chore: add race-enabled CI workflow`
- **Branch Naming**:
  - Tracked work: `<type>/<TICKET-ID>-<description>`
  - Untracked work: `<type>/<description>`
