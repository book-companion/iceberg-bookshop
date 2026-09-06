-- Chapter 8: the fleet-prioritisation score for one table. Run per table and sort.
-- Three terms, all from metadata: files beyond one per partition (what compaction can
-- change), delete rows per data file (what a delete-threshold rewrite removes), and
-- snapshots since the last DATA compaction. A first draft keyed to the 512 MB target
-- rated every small table as 100% small and ranked by file count alone; this does not.
--
-- `operation = 'replace'` is both a data compaction and a manifest rewrite; the summary
-- key `deleted-data-files` tells them apart. Retention bounds the third term: a table
-- that keeps two snapshots reads two whatever happened.
SELECT
  (SELECT count(*) - (SELECT count(*) FROM ice.<ns>.<table>.partitions)
     FROM ice.<ns>.<table>.data_files)                                                  AS excess_files,
  (SELECT coalesce(sum(record_count), 0) FROM ice.<ns>.<table>.delete_files)
     / greatest((SELECT count(*) FROM ice.<ns>.<table>.data_files), 1)                  AS deletes_per_file,
  (SELECT count(*) FROM ice.<ns>.<table>.snapshots
    WHERE committed_at > coalesce((SELECT max(committed_at) FROM ice.<ns>.<table>.snapshots
                                    WHERE operation = 'replace'
                                      AND coalesce(CAST(summary['deleted-data-files'] AS INT), 0) > 0),
                                  timestamp '1970-01-01'))                              AS since_compaction;
-- score = excess_files + deletes_per_file / 100 + since_compaction
