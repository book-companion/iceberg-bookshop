# Chapter 11 — streaming tables: the maintenance schedule and the runbook

Flink 2.1.3 with the Iceberg 1.11.0 runtime. `flink/sql/stream_events.sql` and
`flink/sql/stream_upserts.sql` are the two writers the chapter measured; submit
them with `sql-client.sh -f /scripts/<file>` inside the jobmanager. Each checkpoint
is one commit, one manifest (a fast append never merges), and one data file per
partition touched.

## The schedule, per table class

| Job | Append stream (`events`) | Upsert stream (`orders_cdc`) | Why |
|---|---|---|---|
| `rewrite_data_files` | hourly, `where` covering the lateness window; partial progress on | every N checkpoints, `delete-file-threshold` 1 | files per checkpoint; delete files per checkpoint |
| `rewrite_manifests` | hourly, after compaction | with compaction | one manifest per checkpoint, never merged |
| `expire_snapshots` | no arguments; hot-class properties (`policies/retention.sql`) | same | the metadata file grows ~0.7 KB per checkpoint |
| `remove_orphan_files` | daily, `older_than` past the longest recovery | same | every restore leaves uncommitted files |
| unscoped compaction | weekly | monthly | the late partitions the `where` missed |

## The runbook

| Event | What the table shows | What to do |
|---|---|---|
| TaskManager lost | failed checkpoints, then `restored` > 0; row count exact | nothing; schedule orphan cleanup |
| catalog returned 500 to the writer | job `RESTARTING`; the checkpoint re-commits | fix the catalog; the writer will not lose the checkpoint |
| job resumed from a savepoint | one new `flink.job-id` in `.snapshots` | expected; delete the savepoint |
| the same savepoint resumed twice | a third `flink.job-id`; duplicates in the table | roll back to the first resumed job's last snapshot; take a new savepoint |
| column dropped under a running job | the job keeps committing with the old field | restart the job with the schema change; never re-add the name |
| one-row files in old partitions | `.partitions` shows files in dates the writer is not filling | widen the compaction `where` |
| read cost climbing on an upsert table | `delete_files` count equals the checkpoint count | the data-file rewrite with a delete threshold |

## The audit query

```sql
SELECT summary['flink.job-id'] AS job, count(*) AS commits, min(committed_at), max(committed_at)
FROM ice.<ns>.<table>.snapshots GROUP BY 1 ORDER BY 3;
```

Two overlapping time ranges is two jobs writing at once. A job whose first commit is minutes after another's last is a resume. Three job ids after one savepoint is a replay.
