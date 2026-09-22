# pgcron

PostgreSQL-backed interval jobs for replicated Go applications. Each named job
has one persisted `next_run_at`; replicas share that schedule, not independent
timers. Requires PostgreSQL 14+.

Apply `pgcron.Schema` through your migration system before starting any jobs.
The runtime never creates or alters tables. All replicas must use the same
database, search path, job name, and interval.

```go
job, err := pgcron.New(db, pgcron.Params{
    Name:     "myapp.refresh_search",
    Interval: time.Minute,
    Run: func(ctx context.Context, tx *sql.Tx) error {
        _, err := jobs.EnqueueTx(ctx, tx, queue.EnqueueParams{
            QueueName: "refresh_search",
            Payload:   []byte("{}"),
        })
        return err
    },
})
if err != nil {
    return err
}
return job.Run(ctx, func(result pgcron.Result, err error) {
    if err != nil {
        logger.Error("schedule pass failed", "error", err)
    }
})
```

Use `RunOnce(ctx)` to drive a pass directly in a test or an existing loop.
`Run` blocks until cancellation, reports each pass, and sleeps until the
database's due time. Local wall-clock skew does not decide eligibility.
The caller owns the goroutine and must cancel and join it on shutdown.

## Scheduling contract

- A new job is due immediately. Its first successful transaction establishes
  the cadence; recreating a job or restarting a process does not reset it.
- The advisory lock is held through `Run`. Other replicas return `Busy`.
  A free lock is not enough to run: the stored time must also be due.
- After success, advance along the original cadence to the first time after
  callback completion. Missed occurrences coalesce into one run, without a
  catch-up burst. A job due at 12:00 and resumed at 12:05:20 runs once and is
  next due at 12:06, not 12:06:20.
- Failure leaves the due time unchanged and rolls back writes made through
  the supplied transaction. A peer may retry immediately. A process crash
  releases its lock and rolls back when PostgreSQL detects the disconnect.
- The local loop retries busy/failed passes after `RetryInterval` (default
  one second). This is not cluster-wide retry backoff. For bounded retries,
  use `queue.EnqueueTx` in the callback and let the queue execute the work.
- `AfterRun` runs only after commit and lock release. Its failure returns an
  error with `Outcome == Ran`; the next due time remains committed. This hook
  is best-effort, not durable: a crash after commit may skip it. It may overlap
  a later occurrence on another replica. Enqueue essential work in `Run`.
- Changing `Interval` preserves the already-stored due time and applies the
  new interval when that occurrence succeeds. Deploy consistent parameters
  across replicas; mixed versions can apply either interval during rollout.
- Removed jobs leave an inert row. Reusing the name resumes that schedule.

## Failure limits

This is not exactly-once execution of arbitrary side effects. HTTP calls and
database writes outside the supplied transaction cannot be rolled back. Such
callbacks must be idempotent and respect context cancellation. A lost database
connection can release the lock while an uncooperative callback is still running.
The callback must not commit or roll back the supplied transaction.

Callbacks run with an open transaction and consume a database connection.
Keep them short, preferably just enqueueing work. Allow pool capacity for any
additional connections a callback uses.

Panics propagate after transaction rollback; they are not silently swallowed.
Database errors remain wrapped and observable through the reporting callback.

Intentionally no cron-expression parser, leader election, job history, UI, or
new queue. Those are separate concerns; `pgcron` supplies durable interval
coordination for maintenance passes and atomic job dispatch.

## Verification

```sh
go test -race ./pgcron -count=1
```

Tests use real PostgreSQL, including simultaneous replicas, restart persistence,
missed occurrences, long callbacks, rollback, cancellation, hook ordering, and
transactional dispatch into the existing queue.
