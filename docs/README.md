# pgkit documentation

Start with [agent rules](../AGENTS.md) and [domain vocabulary](../CONTEXT.md). These documents work from an independent clone.

## Current system

- [Architecture](technical/architecture.md): package boundaries, data flow and durability limits.
- [Queue and worker API](../README.md#worker-runtime): typed handles, pickup modes and notifications.
- [Interval scheduling](../pgcron/README.md): schedule ownership, transactions, cadence and failure limits.
- [Workflow runtime](../workflow/README.md): definitions, runs, foreach state and maintenance tasks.

## Development and operations

- [Setup](development/setup.md), [testing](development/testing.md), [troubleshooting](development/troubleshooting.md).
- [Publishing and consumer upgrades](runbooks/deployment.md), [rollback](runbooks/rollback.md).
- [Architectural decisions](adr/README.md).

Add research or postmortems when there is an actual investigation or incident to document. Keep package-specific references beside their source and link them here.
