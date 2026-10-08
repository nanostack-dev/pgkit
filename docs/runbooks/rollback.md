# Roll back a consumer upgrade

1. Identify the failing consumer version, its previous pgkit dependency and the stored payload/schema changes introduced by the upgrade. Check whether the old code can safely process newly stored jobs and workflow definitions.
2. If compatible, restore the previous module version in that consumer with `go get github.com/nanostack-dev/pgkit@vPREVIOUS`, run `go mod tidy` and its affected tests, then deploy using the consumer's rollback procedure.
3. Verify claims, retries, schedule cadence and workflow state through the application's supported tools. Avoid blindly replaying external side effects or deleting durable state.
4. If the old version cannot read current state, use a forward fix or an explicitly reviewed application migration. A binary downgrade does not reverse database changes or completed effects.

Revert faulty source changes in a new pgkit PR and publish a new patch version when ready. Preserve published tags so existing consumers retain a reproducible dependency. Record the verified cause and prevention in project documentation; add a postmortem for a significant incident.
