# workflow

Durable Go functions on PostgreSQL. A workflow is an ordinary function; every
operation it runs through its `*workflow.Context` is checkpointed by name. After a
crash, a deploy, a sleep or a wait for a signal, the function runs again from the
top and completed operations return their recorded results instead of running
twice.

```go
var approved = workflow.NewSignal[Approval]("approved")

var Onboard = workflow.Define("onboard", func(wf *workflow.Context, in Signup) (Account, error) {
	user, err := wf.Step("create-user", func(ctx context.Context) (User, error) {
		return users.Create(ctx, in.Email)
	})
	if err != nil {
		return Account{}, err
	}

	account, err := wf.TxStep("open-account", func(ctx context.Context, tx *sql.Tx) (Account, error) {
		return accounts.InsertTx(ctx, tx, user.ID) // commits with the checkpoint: exactly once
	})
	if err != nil {
		return Account{}, err
	}

	decision, err := wf.Receive(approved, 72*time.Hour)
	if errors.Is(err, workflow.ErrTimeout) {
		return account, wf.Sleep("grace-period", 24*time.Hour)
	}
	if err != nil {
		return Account{}, err
	}

	_, err = wf.Call("welcome", SendWelcome, Email{To: in.Email, ApprovedBy: decision.By})
	return account, err
})
```

```go
client, err := workflow.New(queueClient) // stores runs in the queue client's database
err = client.EnsureSchema(ctx)

worker, err := client.Worker("onboarding").Workflows(Onboard, SendWelcome).Build()
go worker.Run(ctx)

run, err := client.Start(ctx, Onboard, Signup{Email: "ana@example.com"}, workflow.Key("onboard:ana@example.com"))
err = client.Signal(ctx, run.ID, approved, Approval{By: "lea"})
account, err := run.Result(ctx)
```

Requires Go 1.27: the durable operations are generic methods, so types are inferred
from the functions you pass and results come back typed.

## The two rules

1. **Effects go in steps.** Anything that reads or changes the outside world (HTTP
   calls, database writes, the clock, random numbers) belongs inside `Step`,
   `TxStep` or `Async`. Code between steps runs again on every replay.
2. **Control flow follows recorded values.** Branch and loop on inputs and step
   results, not on `time.Now()` or a database read outside a step. Then every
   replay takes the same path and reaches the same checkpoints.

Breaking the second rule is caught when it changes what an operation name records:
a name recorded as a step that the code now uses as a sleep, or a recorded value
that no longer decodes, fails the run with `ErrNonDeterministic`.

## Operations

| Operation | Records | Returns |
| --- | --- | --- |
| `wf.Step(name, fn, opts...)` | `fn`'s result, after retries | `(T, error)`; a `*StepError` after the last attempt |
| `wf.TxStep(name, fn, opts...)` | `fn`'s result in the same transaction as its writes | `(T, error)` |
| `wf.Async(name, fn, opts...)` | like `Step`, run concurrently | `*Future[T]` |
| `wf.Sleep(name, d)` / `wf.SleepUntil(name, t)` | the wake time, measured from the first visit | `error` |
| `wf.Receive(signal, timeout)` | the next signal of that kind | `(T, error)`; `ErrTimeout` when none came |
| `wf.Start(name, child, input, opts...)` | a child run | `*Future[Out]` |
| `wf.Call(name, child, input, opts...)` | `Start` then `Wait` | `(Out, error)`; a `*RunError` when the child failed |

- **Names** identify checkpoints within a run. A name repeated in one execution,
  typically in a loop, is numbered: `charge`, `charge#2`, `charge#3`. Names must not
  contain `#`, and the order of repeated names must be deterministic. Renaming a
  step makes in-flight runs execute it again.
- **Waiting frees the worker.** `Sleep`, `Receive`, a child's `Wait` and a retry
  backoff longer than a second return `ErrSuspended`. Return it from the function
  (wrapping is fine): the run parks without holding a worker and resumes later from
  its checkpoints. After one suspension every later operation returns it too, so a
  swallowed `ErrSuspended` cannot let the function run past the wait.
- **Waits shorter than a second** happen in process instead of parking.
- **Signals** are typed messages declared once with `NewSignal[T](name)` and shared
  by `Client.Signal` and `Context.Receive`. A signal sent before the run waits is
  kept; signals of one kind are received once each, in the order sent. A signal
  sent after a receive's timeout does not count for it, even if the run was still
  parked. Pass `workflow.Forever` to wait without a timeout and `0` to check once.
- **Children** run on whichever worker lists their workflow. Their key is the
  parent's run ID and step name, so a replay never starts them twice. Failing or
  cancelling a parent cancels its unfinished children.
- **Async steps** run concurrently with the function; the run does not finish
  before they do. Call durable operations from the workflow's goroutine, or give
  concurrent calls distinct names.
- **Durable operations inside a step function** are not supported: the step's
  result already covers everything it did.

## Options

Options are typed values. Zero fields take defaults.

| Option | Applies to | Effect |
| --- | --- | --- |
| `workflow.Retry{MaxAttempts, Backoff, MaxBackoff}` | `Define`, steps | Attempts and exponential backoff. Defaults: 3 attempts, 1s doubling to 1m (or to `Backoff` if longer). A step's zero fields keep the workflow's values. |
| `workflow.NoRetry` | steps | One attempt. |
| `workflow.Timeout(d)` | steps, `Start`, child `Start`/`Call` | Bounds one step attempt, or a whole run. A run past its timeout fails; `errors.Is(err, workflow.ErrTimeout)` reports it. |
| `workflow.Key(k)` | `Start` | Idempotent start: a run of the workflow with that key is returned instead of starting another, whatever its status. Children are keyed by their step name and refuse a `Key`. |
| `workflow.Version(n)` | `Define` | Separates incompatible versions; see [Changing a workflow](#changing-a-workflow). Workflow names cannot contain `@`. |

Step functions receive a `context.Context` bounded by `Timeout` and cancelled when
the run is cancelled, times out or moves to another worker. `workflow.IdempotencyKey(ctx)`
returns `"<run id>/<step name>"`, stable across attempts and replays: pass it to
external APIs so a repeated attempt does not repeat their effect.

## Errors

- `NonRetryable(err)` fails a step at once.
- `*StepError{Step, Attempts, Message}` reports a step that used all its attempts.
  It is rebuilt from the database on replay, so it carries the last error's
  message rather than the error value: branching on it behaves the same on every
  replay. A workflow can handle it, for example to compensate, and continue.
- `*RunError{RunID, Workflow, Status, TimedOut, Message}` reports a failed or
  cancelled run from `Run.Result` and from a child's `Wait`.
  `errors.Is(err, workflow.ErrCancelled)` and `errors.Is(err, workflow.ErrTimeout)`
  tell cancellation and timeouts apart.
- A panic in a step is a failed attempt; a panic in the workflow function fails the
  run.
- A run past its `Timeout` fails with `ErrTimeout`, even if its last step finished
  after the deadline.
- Values holding a NUL character (`\u0000`) cannot be stored in PostgreSQL JSONB: a
  step result with one fails the step without retry, and `Start` or `Signal` with
  one returns an error.
- Database errors while recording are not step failures: the activation stops and
  the queue retries it, and the run replays from its checkpoints.

## Exactly once, at least once

- `TxStep` writes and its checkpoint commit together: they happen exactly once.
  The transaction holds a share lock on the run until it ends, so a worker taking
  the run over, or a cancellation, waits for it; signals and child completions do
  not.
- Other steps are **at least once**. A worker that crashes after a side effect but
  before recording it runs the step again on resume; that attempt counts against
  `Retry`. Use `IdempotencyKey` with external systems.
- A step attempt interrupted by a graceful worker shutdown is handed to another
  worker without being counted.

## Clients and workers

```go
run, err := client.Start(ctx, wf, input, opts...)      // its own transaction
run, err := client.StartTx(ctx, tx, wf, input, opts...) // atomic with your writes
run := client.Run(wf, runID)                            // typed handle on an existing run
run, err := client.RunByKey(ctx, wf, key)
output, err := run.Result(ctx)                          // waits, notified on completion

err = client.Signal(ctx, runID, signal, value)          // or SignalTx in your transaction
err = client.Cancel(ctx, runID)                         // run and unfinished descendants
err = client.Retry(ctx, runID)                          // resume a failed or cancelled run
info, err := client.GetRun(ctx, runID)
steps, err := client.ListSteps(ctx, runID)
runs, err := client.ListRuns(ctx, workflow.ListRunsParams{Status: workflow.RunFailed})
deleted, err := client.Purge(ctx, workflow.PurgeParams{OlderThan: 30 * 24 * time.Hour}) // whole finished trees
```

`Retry` keeps completed checkpoints, starts failed and interrupted steps over with
fresh attempts, retries failed or cancelled children, and drops the run's timeout.
After a fix for a non-determinism failure, `Retry` resumes the run on the fixed code.

```go
worker, err := client.Worker("billing").
	Workflows(Checkout, Refund).
	Pickup(queue.OnEnqueue()).           // default when notifications work, else polls every second
	Tune(workflow.WorkerConfig{
		Concurrency:       8,                // runs executed at once (default 4)
		VisibilityTimeout: time.Minute,      // takeover after missed heartbeats (default 30s)
		ReapInterval:      10 * time.Second, // stuck-run and orphan checks
		OnRunFailed:       func(ctx context.Context, run workflow.RunInfo) { alert(run) },
	}).
	Build()
```

A worker executes only the workflows (and versions) it lists. With `OnEnqueue`,
starting, signalling, cancelling and finishing children wake parked runs at once.
lib/pq and other non-pgx drivers need `queueClient.ListenOn(dsn)` for that; without
it the worker polls. `Run.Result` returns on a notification when available and
polls otherwise.

The `fx/workflow` module provides a `*workflow.Client` and can run a worker for the
workflows provided in its `workflowfx.DefinitionsGroup`.

## Changing a workflow

Runs replay with the code deployed now. Safe changes:

- adding steps after the current position of in-flight runs;
- changing the body of a step that in-flight runs have already recorded;
- renaming a step whose re-execution is acceptable.

Incompatible changes (removing or reordering steps that change the outcome,
changing a step's result type) need a new version:

```go
var CheckoutV1 = workflow.Define("checkout", checkoutV1)                     // version 0
var CheckoutV2 = workflow.Define("checkout", checkoutV2, workflow.Version(2))
worker := client.Worker("billing").Workflows(CheckoutV1, CheckoutV2).Build()
```

New runs start with whichever definition you pass to `Start`; a run is executed
only by workers listing the version it started with. Keep the old definition
registered until `ListRuns` shows no unfinished runs of it.

## Testing workflows

Use a real PostgreSQL, as this package's own suite does with testcontainers. Start
the run, run a worker in the test, and await `Result`. To skip a long sleep or
timeout, move the run's due times to now and wake its job, as the suite's
`fastForward` helper does in [main_test.go](main_test.go).

## Upgrading from the DAG package

The `Define(name, func(*Builder))` DAG API, `Publish`/`Activate`, definition
versions and graph diffs are gone; see
[ADR 0001](../docs/adr/0001-code-first-durable-workflows.md). Port each step to
`wf.Step`/`wf.TxStep` in dependency order, replace `DependsOn` with ordinary
sequencing (or `Async`/`Start` for parallel branches), and read previous results
from Go variables instead of `step.Output`. The new tables use the `pgworkflow_`
prefix; the old `workflow_*` tables and their `workflow.internal.step` queue jobs
are left untouched. Drain or abandon old runs, then drop those tables in an
application migration.

## How it works

For maintainers. Tables: `pgworkflow_runs`, `pgworkflow_steps` (checkpoints, primary
key run and name) and `pgworkflow_signals`, created by `EnsureSchema`
([schema.go](schema.go)).

- **One job per run.** Each run owns one queue job (`runs.job_id`) on the queue
  `pgworkflow:<name>` or `pgworkflow:<name>@<version>` (names cannot contain `@`). A
  parked run snoozes the job (`queue.SnoozeTx`, attempt not counted) until its
  earliest wake time, or until woken.
  Signals, child completions and cancellations make it due with `queue.WakeTx`,
  which notifies at commit.
- **Activations and fencing.** Claiming the job starts an activation, which bumps
  `runs.lease` while holding the job's claim (`queue.LockClaimTx`), so a worker whose
  claim the reaper took back cannot displace the current one; finishing holds the
  claim too. Every checkpoint write runs in a transaction that share-locks the
  run row and checks the lease and the `running` status ([activation.go](activation.go)),
  so a worker that lost the run cannot write, and a takeover waits for an in-flight
  write. Snoozes are fenced by the job's attempt number.
- **One park point.** Operations that must wait record why and return
  `ErrSuspended`. When the function returns, the park transaction locks the run
  `FOR UPDATE` and re-checks every condition (unreceived signals, finished
  children, due times); if one is met the function runs again at once, otherwise
  the run becomes `waiting` and its job is snoozed. Senders lock the run
  `FOR KEY SHARE`, which serializes them with the park, so no wakeup is lost.
- **Lock order.** A transaction touching two runs locks the parent before the
  child; cancellation locks a tree from the top down and repeats its lookup until
  no unfinished descendant remains.
- **Attempts.** A step is marked `running` (attempts + 1) before it executes, so a
  crash counts as a failed attempt. Failure records are guarded by the attempt
  number. Backoffs up to a second wait in process; longer ones park with
  `status = 'retrying'` and `wake_at`.
- **Time.** Due times are database times. An activation reads the remaining
  duration once and waits on the monotonic clock.
- **Watching.** A watcher heartbeats the job every third of the visibility timeout
  until the activation ends, and listens for the run's notifications, cancelling
  the run context when the run is cancelled or taken over.
- **Shutdown.** Workers claim and settle jobs on contexts shutdown does not cancel,
  hand back jobs claimed while stopping, record steps that finished, and refund an
  interrupted attempt.
- **Reconciling.** Each worker periodically fails unfinished runs no activation can
  finish: their job failed (attempts exhausted by crashes or reaps), was settled
  outside the runtime, or was deleted. `OnRunFailed` is called for them too.

The queue primitives this relies on are documented with the queue in the
[repository README](../README.md).
