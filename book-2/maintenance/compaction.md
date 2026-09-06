# Chapter 8 — compaction as a policy

The decision tree, the candidate prediction, and the fleet score. Every rule
here was measured in the chapter; the numbers it leans on are in the book.

## The decision tree, per table, in order

1. **Is there anything to do?** Run `candidate_prediction.sql`. Zero candidates, no job. A compaction the prediction says is a no-op exits green and tells you nothing.
2. **Scope it.** `where => "<partition column> >= '<recent>'"` on the partitions the writer is still filling. History is compacted once. Widen the `where` to cover the source's lateness window (chapter 11).
3. **Binpack**, unless a measured predicate on *this* table justifies a sort, and the partitions are large enough that a sort gives a reader something to skip. On partitions of a few hundred kilobytes, sort and z-order are a more expensive binpack.
4. **Partial progress on**, with `partial-progress.max-commits` sized to what the catalog can take in a burst. Verify the result from the snapshot log and the file count, never from the procedure's return row: it reports refused commits as zero failed groups.
5. **Deletes:** on v2, `delete-file-threshold` for the data rewrite and a `rewrite_position_delete_files` with `rewrite-all` after it (the rewritten data files keep the starting sequence number, so the old position deletes are not "dangling" by the sequence rule). On v3, the data rewrite alone removes the deletion vectors.
6. **Schedule against the writers.** Appends overlap safely. Row-level writers on the same partitions cannot: whichever commits second loses, with `Missing required files to delete`, and which side that is comes down to milliseconds.
7. **Expire and clean orphans after** (chapters 9, 10, 14). A catalog that returned a 500 mid-commit has produced orphans by design.

## Defaults (Iceberg 1.11.0, `javap -constants` on the runtime jar)

| Option | Default |
|---|---|
| `target-file-size-bytes` | the table's `write.target-file-size-bytes`, 512 MB if unset |
| `min-file-size-bytes` / `max-file-size-bytes` | 0.75 × and 1.8 × the target |
| `min-input-files` | 5 |
| `rewrite-all` | false |
| `partial-progress.enabled` / `partial-progress.max-commits` | false / 10 |
| `max-concurrent-file-group-rewrites` | 5 |
| `max-file-group-size-bytes` | 100 GB |
| `use-starting-sequence-number` | true |
| `delete-file-threshold` | `Integer.MAX_VALUE` |
| `delete-ratio-threshold` | 0.3 |
| `remove-dangling-deletes` | false |

## Files here

- `candidate_prediction.sql` — what a default compaction will rewrite, from metadata, before it runs.
- `fleet_score.sql` — files beyond one per partition, delete rows per data file, snapshots since the last data compaction; a ranking, not a measurement.
