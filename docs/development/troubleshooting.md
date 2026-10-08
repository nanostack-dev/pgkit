# Development troubleshooting

## PostgreSQL tests cannot start

The database suites provision containers. Check `docker info` and whether the current shell can reach the configured Docker socket. Restore daemon/socket access, then rerun the same package with `-race -count=1`. A sandbox socket denial is missing verification, not evidence of a product defect.

## Notification worker cannot build with a non-pgx driver

`OnEnqueue` needs a dedicated LISTEN connection outside the SQL pool. The default configuration can derive it from pgx stdlib; other drivers need `client.ListenOn(...)` or `client.ListenWith(...)` before building workers. See [listener setup](../../README.md#picking-up-jobs) and [regression coverage](../../queue/pickup_test.go). Confirm `Build` succeeds and the worker's `Ready()` channel closes after its initial successful scan.

## UI edits do not appear in the playground

Go serves the embedded `adminui/dist` snapshot, rather than Svelte source or `ui/embedded/build`. Rebuild and copy assets using [setup](setup.md), rebuild/restart the Go process, and verify the changed view. Include both source and generated assets in the PR.

## Scheduled callback repeats after failure

A failed schedule transaction leaves its due time unchanged; another replica can retry immediately. Essential side effects should be transactional enqueue operations, and external calls must be idempotent. Inspect the [scheduling contract](../../pgcron/README.md#scheduling-contract) before changing cadence or retry behavior; verify the affected pgcron tests.

Add a new entry only after its symptom, cause, repair and verification are established.
