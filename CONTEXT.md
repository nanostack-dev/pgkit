# pgkit context

pgkit supplies coordination and durable execution primitives. Applications own their domain objects, queue names, payload validation and external side effects.

## Language

**Queue:** A named stream of jobs sharing a compatible payload contract and handler. Producers and workers using one name must agree on that contract.

**Job:** One durable request for work, including its payload, availability, claim and attempt history. A job is pending, processing, done or failed.

**Pickup:** The policy governing when a worker searches for claimable jobs: notification-driven with periodic rescans, or polling.

**Claim:** Temporary ownership of a processing job by a worker. It ends with acknowledgement, retry, failure or stuck-job recovery.

**Schedule:** A named durable interval cadence shared by replicas. Restarting a process does not create a separate cadence.

**Workflow definition:** A versioned graph of steps and dependencies. Published definitions and individual executions are separate concepts.

**Workflow run:** One execution of a workflow definition with its own input, state and step results.

**Step:** One logical unit in a workflow graph. A foreach step may materialize several item executions.

**Replay:** A deliberate return of existing work to executable state. Its side effects must be safe to repeat.
