# Architecture

pgkit is a Go module embedded in its consumers, rather than an independently deployed service. [go.mod](../../go.mod) owns its toolchain and dependencies.

| Area | Responsibility | Boundary |
| --- | --- | --- |
| `pglock` | Transaction and session advisory locks | The caller supplies a database and owns business work. |
| `queue` | Durable jobs, claim/ack/retry/fail/reap and worker runtime | Applications own names, compatible payloads and handler semantics. |
| `pgcron` | Durable interval cadence across replicas | The application migrates the schema and owns callback side effects. |
| `workflow` | Durable Go functions: runs, named checkpoints, signals, child runs | The `pgworkflow_*` tables own execution state; one queue job per run drives it. |
| `adminui` | Embedded Svelte assets and authenticated monitoring/mutation API | The application chooses exposure, token and enabled mutations. |
| `fx` | Optional Uber Fx wiring for these primitives | Consumers may use the core packages without this wiring. |

## Execution and durability

Producers persist jobs, optionally within an application transaction. Workers claim available jobs using `FOR UPDATE SKIP LOCKED`; multiple replicas can share one queue. Successful handlers acknowledge jobs, retryable failures reschedule them, and exhausted or nonretryable failures become terminal. A reaper recovers expired processing claims.

Notification pickup uses `LISTEN/NOTIFY` as a wake-up hint and periodic rescans as recovery. Notifications are delivered at commit. A pgx stdlib connection can supply the listener configuration; other drivers need an explicit listener. See the [worker API](../../README.md#picking-up-jobs) and [pickup tests](../../queue/pickup_test.go).

A workflow run's state lives in `pgworkflow_runs`, `pgworkflow_steps` (checkpoints keyed by run and operation name) and `pgworkflow_signals`. Each run owns one queue job: an activation claims it, re-executes the workflow function against the recorded checkpoints, and finishes, parks (snoozing the job) or releases the run. Leases fence checkpoint writes; signals, child completions and cancellations wake parked runs through `WakeTx` and notifications. The [workflow guide](../../workflow/README.md#how-it-works) owns the detailed invariants and [ADR 0001](../adr/0001-code-first-durable-workflows.md) the rationale. The earlier DAG package's `workflow_*` tables are left to applications to drop. Schedules persist their cadence separately and use database time; the [pgcron contract](../../pgcron/README.md) distinguishes committed work from best-effort post-commit hooks.

Durability does not make arbitrary HTTP calls or other external effects exactly once; only a workflow's transactional steps commit their writes with their checkpoint. Consumers must choose idempotency, retry and shutdown behavior. Schema changes and rolling upgrades must preserve both stored work and old/new producer compatibility.

## Administration

The dashboard requires token authentication, keeps mutation endpoints configurable, and must preserve its existing CSRF protections. Server errors return only their status text, never internal database errors or secrets. Workflow screens list runs, show each run's checkpoints, children, input and output, and (with mutations enabled) retry a failed run from its checkpoints or cancel an unfinished one. The shipped assets are embedded from [adminui/dist](../../adminui/dist); frontend source is in [ui/embedded](../../ui/embedded). Asset rebuilds must update that embedded output as described in [setup](../development/setup.md).
