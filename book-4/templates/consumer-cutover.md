# Consumer cutover worksheet

Status: authored template; no consumer has been migrated or accepted.

For each report/job/view, record:

| Field | Required evidence |
|---|---|
| Consumer identity and owner | Confirmed application/report owner |
| Source and target connection | Actual engine/catalog/database and authenticated identity |
| Data boundary | Source capture and corresponding target snapshot |
| SQL/view definition | Versioned source and target dialects, parameters, session timezone |
| Result contract | Columns, types, duplicates, ordering, nulls and any explicit tolerance |
| Refresh behavior | Schedule, timezone, cache invalidation, watermark and late data |
| Side effects | Exports, notifications, dependent jobs and retry/idempotency behavior |
| Go/stop conditions | Accepted reports, denial controls, rollback authority |
| Recovery | Before-target-write routing and after-target-write reconciliation |

## dbt extension

Compile the selected model using the actual target adapter in a separate target
schema. Preserve compiled SQL and adapter versions. Execute the model and its
key/null/business tests at the declared boundary, then compare its report output
with the source model. Check incremental unique keys, merge/delete semantics,
full-refresh behavior and job credentials separately. This worksheet does not
supply or claim a tested dbt adapter project.

## BI extension

Create a separate connection/report copy. Verify driver types, parameter binding,
timezone, pagination and sorting. Clear or label caches, trigger a real refresh,
and inspect what the intended viewer sees under that viewer's identity. A SQL
comparison alone does not establish dashboard delivery. Retain the refresh id,
observed data boundary and owner acceptance before routing production users.

## Engine-specific view extension

Capture the source view's definition and dialect. Create and query a target view
through each intended engine rather than assuming catalog registration makes the
definition portable. Compare authorization through the view and directly through
the base table. Keep unsupported engine paths blocked instead of silently
replacing the view with a broader table grant.
