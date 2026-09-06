-- Chapter 10: the retention policy template. Every table is in a class, a class is a
-- property set, and anything a class does not cover is a tag or a branch with its own
-- lifetime. Then one scheduled job per table with NO arguments:
--   CALL ice.system.expire_snapshots(table => '<ns>.<table>');
-- The policy lives in the table and its refs, where the next reader of the metadata finds it.

-- hot: recovery window only. Set the age from the recovery requirement, the count as a floor.
ALTER TABLE ice.<ns>.<events> SET TBLPROPERTIES (
  'bookshop.class' = 'stream',
  'history.expire.max-snapshot-age-ms' = '86400000',      -- 1 day
  'history.expire.min-snapshots-to-keep' = '2',
  'write.metadata.previous-versions-max' = '10',
  'write.metadata.delete-after-commit.enabled' = 'true'   -- set at creation; files that already dropped off the log are orphans
);

-- audit: hot's properties plus tags on the boundaries someone signs. A tag pins a full
-- copy of the table through every later compaction: month ends, not days.
ALTER TABLE ice.<ns>.<orders> SET TBLPROPERTIES (
  'bookshop.class' = 'batch',
  'history.expire.max-snapshot-age-ms' = '604800000',     -- 7 days
  'history.expire.min-snapshots-to-keep' = '5',
  'write.metadata.previous-versions-max' = '10',
  'write.metadata.delete-after-commit.enabled' = 'true'
);
ALTER TABLE ice.<ns>.<orders> CREATE TAG month_end_2026_08 AS OF VERSION <snapshot_id> RETAIN 2555 DAYS;   -- seven-year records
ALTER TABLE ice.<ns>.<orders> CREATE TAG week_end_2026_36  AS OF VERSION <snapshot_id> RETAIN 30 DAYS;     -- a hold with an end date

-- dev: a branch with both a count and an age. The count is what the work needs; the age
-- is when the branch stops needing anyone to remember it. Syntax order matters.
ALTER TABLE ice.<ns>.<customers> SET TBLPROPERTIES ('write.wap.enabled' = 'true');   -- without it, WAP sessions write to main
ALTER TABLE ice.<ns>.<customers> CREATE BRANCH feature_x RETAIN 7 DAYS WITH SNAPSHOT RETENTION 10 SNAPSHOTS;

-- Read the policy back — it is the only place it is stored:
SELECT name, type, snapshot_id, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms
FROM ice.<ns>.<orders>.refs;

-- Main's retention has to exceed the longest incremental-consumer lag (chapter 12): a reader
-- whose start snapshot expires gets "Starting snapshot ... is not a parent ancestor", not a late delta.
