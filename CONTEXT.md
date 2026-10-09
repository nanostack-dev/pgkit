# pgkit context

pgkit supplies coordination and durable execution primitives. Applications own their domain objects, queue names, payload validation and external side effects.

## Language

**Queue:** A named stream of jobs sharing a compatible payload contract and handler. Producers and workers using one name must agree on that contract.

**Job:** One durable request for work, including its payload, availability, claim and attempt history. A job is pending, processing, done or failed.

**Pickup:** The policy governing when a worker searches for claimable jobs: notification-driven with periodic rescans, or polling.

**Claim:** Temporary ownership of a processing job by a worker. It ends with acknowledgement, retry, failure or stuck-job recovery.

**Schedule:** A named durable interval cadence shared by replicas. Restarting a process does not create a separate cadence.

**Workflow:** A named durable Go function from an input to an output, optionally versioned. Its code is the definition; nothing is published to the database.

**Workflow run:** One execution of a workflow with its own input, checkpoints and result. A run is pending, running, waiting, succeeded, failed or cancelled. A key makes starting a run idempotent.

**Checkpoint:** The recorded outcome of one durable operation of a run, identified by name (repeats are numbered `name#2`). Kinds: step, sleep, signal and child.

**Step:** A function a run executes once and records, retried as its policy allows. A transactional step commits its writes with its checkpoint.

**Activation:** One execution of a run by a worker, from claiming the run's job to finishing, parking or releasing it. Its lease fences writes from any activation a takeover superseded.

**Park:** A run waiting without holding a worker: its job is snoozed until its earliest wake time or until something it waits for wakes it.

**Signal:** A typed message sent to a run and received by its workflow in order, kept until received.

**Child run:** A run started by another run's step and keyed by it; its result becomes the parent's checkpoint.

**Replay:** A deliberate return of existing work to executable state. Its side effects must be safe to repeat.

**Workflow replay:** Executing a run's workflow function again from the top, as each activation does; completed checkpoints return their recorded results instead of repeating. Unlike a job replay it is routine, not an operator action. Resuming a failed run is a retry.
