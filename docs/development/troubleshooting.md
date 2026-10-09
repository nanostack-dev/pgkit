# Development troubleshooting

## PostgreSQL tests cannot start

The database suites provision containers. Check `docker info` and whether the current shell can reach the configured Docker socket. Restore daemon/socket access, then rerun the same package with `-race -count=1`. A sandbox socket denial is missing verification, not evidence of a product defect.

## Notification worker cannot build with a non-pgx driver

`OnEnqueue` needs a dedicated LISTEN connection outside the SQL pool. The default configuration can derive it from pgx stdlib; other drivers need `client.ListenOn(...)` or `client.ListenWith(...)` before building workers. See [listener setup](../../README.md#picking-up-jobs) and [regression coverage](../../queue/pickup_test.go). Confirm `Build` succeeds and the worker's `Ready()` channel closes after its initial successful scan.

## UI edits do not appear in the playground

Go serves the embedded `adminui/dist` snapshot, rather than the frontend source or `ui/embedded/build`. Rebuild and copy assets using [setup](setup.md), rebuild/restart the Go process, and verify the changed view. Include both source and generated assets in the PR.

## The admin dev server shows request errors

`pnpm --dir ui/embedded dev` proxies `/api` to `PGKIT_ADMIN_API` (default `http://127.0.0.1:18083`). A 502 or "Network error" means nothing listens there: start the playground with `PGKIT_PLAYGROUND_ADDR=127.0.0.1:18083`, or point `PGKIT_ADMIN_API` at your server and `PGKIT_DASHBOARD_TOKEN` at its token. To work without a backend, open the page with `?data=demo`. Verify that the overview loads its counts.

## Scheduled callback repeats after failure

A failed schedule transaction leaves its due time unchanged; another replica can retry immediately. Essential side effects should be transactional enqueue operations, and external calls must be idempotent. Inspect the [scheduling contract](../../pgcron/README.md#scheduling-contract) before changing cadence or retry behavior; verify the affected pgcron tests.

## A short sleep or retry waits seconds in a test

A run parked until a due time is picked up by the worker's next rescan, as a delayed job is not notified when it falls due. Waits up to a second happen in process, but longer ones depend on `Pickup`. Tests use `queue.OnEnqueue().RescanEvery(100 * time.Millisecond)` or fast-forward the run (see [testing](testing.md#workflow-suite)); production keeps the default rescan.

## Jobs reaped while their handler is still running

Before this was fixed, visibility timeouts, retry delays and purge ages were truncated to whole seconds, so a sub-second visibility timeout reaped every processing job at once. They now keep sub-second precision (`TestReapStuckJobsHonorsASubSecondVisibilityTimeout`). If a long handler is still reaped, heartbeat it with `client.Heartbeat` more often than the visibility timeout; workflow activations already do.

Add a new entry only after its symptom, cause, repair and verification are established.
