# 0001: Code-first durable workflows

Status: accepted (2026-10-08), implementation in progress on `feat/workflow-v2`.

## Decision

Replace the DAG package (`Define/Builder/Publish/Activate`, graph diff) with durable Go functions
whose steps are checkpointed in PostgreSQL by name. Breaking change approved by the owner;
echopoint's openapi-sync is migrated in a companion PR. New tables use the `pgworkflow_` prefix,
so the old `workflow_*` tables stay untouched (apps drop them after draining).

Research basis: DBOS Go, Absurd, pgflow, Hatchet, River, Temporal, go-workflows, Restate, Inngest,
Cloudflare Workflows, Vanlightly's determinism essays, Brandur's idempotency/job-drain posts.

## API (Go 1.27 generic methods)

```go
var Approved = workflow.NewSignal[Approval]("approved")

var Onboard = workflow.Define("onboard", func(wf *workflow.Context, in Signup) (Account, error) {
    user, err := wf.Step("create-user", createUser, workflow.Retry{MaxAttempts: 5}, workflow.Timeout(30*time.Second))
    acct, err := wf.TxStep("open-account", openAccount)       // checkpoint commits with the tx: exactly once
    approval, err := wf.Receive(Approved, 72*time.Hour)       // errors.Is(err, workflow.ErrTimeout); workflow.Forever
    err = wf.Sleep("cool-off", 24*time.Hour)                  // also SleepUntil
    profile := wf.Async("fetch-profile", fetchProfile)        // in-process parallel step -> *Future[T]
    child := wf.Start("provision", Provision, req)            // child run -> *Future[Out]; wf.Call = Start+Wait
    p, err := profile.Wait()
    return acct, err
}, workflow.Version(2), workflow.Retry{MaxAttempts: 3})

client, err := workflow.New(queueClient)                      // DB from queueClient.DB()
run, err := client.Start(ctx, Onboard, in, workflow.Key("onboard:"+email), workflow.Timeout(time.Hour))
run, err := client.StartTx(ctx, tx, Onboard, in)
err = client.Signal(ctx, run.ID, Approved, Approval{By: "lea"}) // also SignalTx
acct, err := run.Result(ctx)                                    // *RunError, ErrCancelled
client.Run(Onboard, id); client.RunByKey(ctx, Onboard, key)
client.Cancel / Retry (resume from failure) / GetRun / Steps / ListRuns / Purge
worker, err := client.Worker("onboarding").Workflows(Onboard, Provision).Pickup(queue.OnEnqueue()).Tune(workflow.WorkerConfig{Concurrency: 8}).Build()
workflow.IdempotencyKey(ctx) // inside a step: "<run id>/<step key>"
```

Options are sealed typed values: `Retry` (step + define default), `Timeout` (step attempt + run),
`Key` (start), `Version` (define). Step errors are always `*StepError{Step, Attempts, Message}`,
reconstructed from storage, so first execution and replay behave the same.

## Runtime rules

- Step key = name, repeats get `name#2`, `name#3` (`#` forbidden in names). Stored kind is checked
  on replay: a mismatch fails the run with `ErrNonDeterministic`.
- One queue job per run (`runs.job_id`), queue `pgworkflow:<name>` (`@<version>` when versioned),
  so workers drain only their versions. Waiting = `queue.SnoozeTx` (attempt not consumed, zero time =
  until woken); signal/child completion/cancel = `queue.WakeTx` + NOTIFY.
- Activation start bumps `runs.lease`; every write locks the run `FOR SHARE` and checks
  `lease` + `status='running'` (fencing). External mutators (signal, cancel, child completion) lock
  `FOR UPDATE`, so they serialize with the single end-of-activation park.
- Suspending ops record why and return `errSuspended`; after one suspension every durable op returns
  it (guards swallowed errors). Park tx re-checks signals/children/wake times; if ready, re-execute
  in-process instead of parking.
- Step: `running` marker (attempts+1) before executing, so a crash counts as a failed attempt.
  Backoff <= ~1s retries in-process, longer parks with `status='retrying', wake_at`.
- DB time for all wake times; local monotonic clock only for the remainder after one DB reading.
- Heartbeat (`queue.Heartbeat`) every VisibilityTimeout/3 + per-run NOTIFY subscription detect
  cancellation and lost leases, cancelling the activation context.
- Failed or cancelled runs cancel non-terminal descendants; terminal transitions wake the parent and
  NOTIFY `pgworkflow:<run id>` for `Result`.

## Schema

`pgworkflow_runs` (id uuidv7, workflow, version, key unique per workflow, status
pending|running|waiting|succeeded|failed|cancelled, input/output/error jsonb, parent_run_id,
parent_step, job_id, lease, deadline_at, wake_at, timestamps), `pgworkflow_steps`
(PK run_id+name, kind step|sleep|signal|child, status running|waiting|retrying|succeeded|failed|timed_out,
attempts, output, error, wake_at, child_run_id, signal), `pgworkflow_signals` (id, run_id, name,
payload, received_by, created_at; partial index on unreceived).

## Remaining work

1. queue: `SnoozeTx`, `WakeTx`, `Heartbeat`, `Subscribe`/`NotifyTx` + tests.
2. workflow package rewrite + exhaustive testcontainers tests (one container per package, DB per test, `-race`).
3. fx module, adminui (step timeline), playground, example, READMEs.
4. echopoint companion PR (openapisyncs) after a pgkit release.
