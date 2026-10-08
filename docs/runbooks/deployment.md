# Publish and upgrade consumers

pgkit deploys through versioned Go module dependencies in consuming applications. This repository has no production service to restart.

1. Review API, payload and schema compatibility and run the checks in [testing](../development/testing.md). Coordinate schema or behavior changes with affected consumers.
2. Merge the approved change. The [Release Patch workflow](../../.github/workflows/release-patch.yml) creates the next patch tag on every push to `main`; it does not wait for CI. Check the CI result and confirm the tag's commit before recommending it to a consumer.
3. In each consumer, update to the reviewed immutable version with `go get github.com/nanostack-dev/pgkit@vX.Y.Z`, then `go mod tidy`. Review `go.mod` and `go.sum`.
4. Run that consumer's affected integration tests and normal release gates. Deploy through its own runbook; verify job processing, scheduling and workflow progress using the consumer's permitted observability.

Consumers upgrading from the DAG workflow package port their definitions as described in [upgrading from the DAG package](../../workflow/README.md#upgrading-from-the-dag-package). The new runtime creates `pgworkflow_*` tables and never reads the old ones; drain or abandon old runs before dropping them.

Check release provenance with `git rev-parse vX.Y.Z^{commit}` and the associated CI run. A tag existing is insufficient evidence that consumers have upgraded or that their deployments are healthy. Keep existing tags immutable; publish fixes as a new version.
