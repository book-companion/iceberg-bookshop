# Chapter 14 — incident runbooks

Eight incidents, each produced on the platform and worked to recovery in the
chapter. The two queries under each row are the triage; the metadata-versus-store
subtraction is the detector for four of the eight.

| Incident | Detect | Triage | Contain | Recover | Keep |
|---|---|---|---|---|---|
| stray metadata file | store listing minus `metadata_log_entries` | nothing points at it | nothing | scheduled orphan cleanup | the listing |
| data file gone | first column read fails; `count(*)` does not | `all_entries` for the path: added in which snapshot | list every file the same rule removed | rollback if a prior snapshot exists; else tag, replica or source | the error, the listing, the snapshot id |
| accidental overwrite | row count; `overwrite` in the snapshot log | the previous snapshot id | stop the writers before the window closes | `rollback_to_snapshot`, 0.1 s | the return row's two pointers |
| dropped column | column unresolvable; a re-add reads null | field ids; the pre-drop snapshot | stop writers carrying the old field | `INSERT OVERWRITE … FROM t VERSION AS OF <pre-drop>` | the field-id listing |
| stale pointer | pointer version vs newest file | metadata-log length | block writes: a write forks the history | `register_table` at the newest file | both forked files |
| corrupt metadata | every engine fails to load; one retries forever | the previous entry in the log | find what truncated it | restore the object; else register the previous file | the corrupt object |
| expired recovery point | `Cannot roll back to unknown snapshot id` | `refs`: any tag or branch that held it | review retention against recovery | tag, replica, or source | the retention properties as they were |
| cleanup vs a live file | cannot happen from SQL (24 h guard, no override) | the interval, the longest write | code-review any Action API cleanup | `add_files` if the file still exists; else source | the cleanup job's interval |

## Triage queries

```sql
-- which snapshots reference a path (data file gone)
SELECT snapshot_id FROM ice.<ns>.<table>.all_entries WHERE data_file.file_path = '<path>' AND status < 2;

-- the previous snapshot (accidental overwrite)
SELECT snapshot_id, operation, committed_at FROM ice.<ns>.<table>.snapshots ORDER BY committed_at DESC LIMIT 2;

-- field ids (dropped column): the re-added column has a new id; the data is under the old one
-- (PyIceberg) [(f.name, f.field_id) for f in catalog.load_table('<ns>.<table>').schema().fields]

-- the metadata log vs the store (stray file, stale pointer)
SELECT file FROM ice.<ns>.<table>.metadata_log_entries ORDER BY timestamp DESC;
-- compare with:  aws s3 ls s3://warehouse/<ns>/<table>/metadata/ | grep metadata.json
```

## Three rules that held in every incident

- **Rollback moves the pointer and nothing else.** Not the schema, not the properties. Time travel reads with the snapshot's schema.
- **Stop the writers before recovering.** Every recovery was one call; every one could have been made impossible by a writer committing past the state it needed.
- **A metadata-only answer is not a health check.** `count(*)` was right with a file missing. Add one column read per table to the health pack.
