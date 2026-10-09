# 0001: Code-first durable workflows

Status: accepted (2026-10-08)

## Context

The `workflow` package ran DAG definitions: steps declared with `DependsOn`, published
and activated as versioned graphs, exchanging untyped JSON (`any`,
`StepContext.Output(name, &target)`). It had no durable sleep, no way to wait for an
external event, no idempotent start, no cancellation or deadlines, and no loops or
conditional branches. Its only consumer, Echopoint's two-step OpenAPI sync, also had
to publish, list and activate definitions by hand at startup.

Go 1.27 allows generic methods on concrete types, which makes a fully typed API
possible without code generation.

Research covered DBOS Transact Go, Absurd, pgflow, Hatchet, River Pro, Temporal,
go-workflows, Restate, Inngest, Cloudflare Workflows and Vercel's Workflow DevKit, and
essays on determinism, idempotency keys and fencing (Vanlightly, Ronacher, Brandur,
Kleppmann).

## Decision

Replace the DAG package with durable Go functions:

- A workflow is `workflow.Define(name, func(*workflow.Context, In) (Out, error))`. Durable
  operations are generic methods on the context: `Step`, `TxStep`, `Async`, `Sleep`,
  `SleepUntil`, `Receive` (typed signals) and child runs through `Start`/`Call`.
- Operations are checkpointed by name; repeated names are numbered (`name#2`). Each
  activation re-executes the function from the top and completed checkpoints return
  their recorded result. A checkpoint recorded under another kind, or a value that no
  longer decodes, fails the run with `ErrNonDeterministic`.
- Each run owns one queue job. A waiting run snoozes it; senders wake it. Activations
  bump a lease that fences every checkpoint write.
- `TxStep` commits user writes with the checkpoint, and `StartTx`/`SignalTx` join the
  caller's transaction: the PostgreSQL-only guarantees other engines lack.
- New `pgworkflow_*` tables; the old `workflow_*` tables are not migrated.

The breaking change was approved by the repository owner, with Echopoint migrated in a
companion change.

## Alternatives

- **Typed DAG:** keep declared graphs and type the edges with generic refs. It keeps the
  graph view and version diffs, but sleeps, signals, loops and branches would each need
  graph constructs, and joins of differently typed parents need `Join2`/`Join3`.
- **Both models side by side:** nothing breaks, but the package would carry two runtimes,
  two schemas and two admin views.
- **Full-history replay (Temporal style):** detects any divergence but makes every change
  to in-flight code a versioning problem and requires strict determinism. Checkpoints by
  name tolerate added steps and keep the determinism burden to two rules.
- **Positional checkpoints (DBOS style):** reorderings break in-flight runs; names are
  more forgiving and readable in the admin UI.

## Consequences

- Workflows read as plain Go, with typed inputs, outputs, signals and children.
- Steps other than `TxStep` are at least once; users pass `IdempotencyKey` to external
  systems.
- Incompatible changes need `workflow.Version(n)` and both definitions registered until
  old runs finish. There is no static graph to diff.
- The admin UI shows a checkpoint timeline instead of a graph.
- Consumers port their definitions and drop the old tables after draining them.
- The queue gained `SnoozeTx`, `WakeTx`, `Heartbeat`, `Subscribe`/`NotifyTx` and scoped
  `Reap`, which other job types can use too.
