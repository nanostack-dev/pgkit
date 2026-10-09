# Diagnose a stuck or failing workflow run

Use the consumer's admin UI (pgkit `adminui`) or a `*workflow.Client` against the
consumer's database. Never edit `pgworkflow_*` or `pgqueue_jobs` rows by hand in
production: the runtime's fencing assumes its own transitions.

## Diagnose

1. Read the run: `client.GetRun(ctx, runID)` gives status, error, `timed_out`,
   `wake_at` and `deadline_at`. The admin UI shows the same on the run page.
2. Read its checkpoints: `client.ListSteps(ctx, runID)`. The last checkpoint that is
   not `succeeded` is where the run is:
   - `waiting` sleep: wakes at `wake_at`.
   - `waiting` signal: waits for that signal name until `wake_at` (none: forever).
   - `waiting` child: open the child run (`ChildRunID`) and diagnose it.
   - `retrying` step: the last attempt's `error`, next attempt at `wake_at`.
   - `running` step on a run that is not running: the attempt's worker stopped; the
     next activation counts it as a failed attempt.
3. A run `running` with no worker activity: check that a worker lists its workflow
   **and version** (`RunInfo.Version`) and that workers are healthy. A worker reaps
   stuck activations after its `VisibilityTimeout`.
4. A run `waiting` past its `wake_at`: the worker rescans parked jobs at its
   `Pickup` interval; check that a worker for the workflow and version runs.

## Repair

- Waiting for a signal that never came: send it with `client.Signal`, or cancel.
- Failed after an external outage or a fixed bug: `client.Retry(ctx, runID)` resumes
  from the checkpoints. Completed steps do not run again; failed and interrupted
  steps start over, and failed or cancelled children are retried.
- Failed with `ErrNonDeterministic`: deploy code whose operation names and result
  types match the recorded checkpoints (or rename the changed step), then `Retry`.
- No longer wanted: `client.Cancel(ctx, runID)` cancels it and its unfinished
  children.
- Failed with "the queue gave up on the run's job": its job exhausted its attempts
  through crashes or reaps. Find the cause in the consumer's logs before `Retry`.

## Verify

`client.Run(workflow, runID).Result(ctx)` returns the output, or the run page shows
`succeeded`. For a cancelled tree, every descendant shows `cancelled` and no job of
the tree stays `processing`.
