-- Chapter 13: the alert rules, as views over ops.table_health (see table_health.py --store).
-- Thresholds are set from the fleet's own distribution — re-derive them when the fleet changes.
-- The class column is declared (table property bookshop.class), never inferred from cadence:
-- inferring a stream from commit gaps fired on six batch tables in the book's run.

CREATE OR REPLACE VIEW ice.ops.latest AS
SELECT * FROM ice.ops.table_health WHERE run_at = (SELECT max(run_at) FROM ice.ops.table_health);

CREATE OR REPLACE VIEW ice.ops.alert_small_files AS      -- action: rewrite_data_files (ch8)
SELECT tbl, excess_files, median_file_kb FROM ice.ops.latest WHERE excess_files > 20 AND median_file_kb < 32768;

CREATE OR REPLACE VIEW ice.ops.alert_delete_debt AS      -- action: rewrite_data_files with delete-file-threshold (ch8, ch11)
SELECT tbl, delete_files, deletes_per_file FROM ice.ops.latest WHERE deletes_per_file > 100 OR delete_files > 10;

CREATE OR REPLACE VIEW ice.ops.alert_compaction_lag AS   -- action: schedule the compaction that is not running (ch8)
SELECT tbl, since_compaction FROM ice.ops.latest WHERE since_compaction > 50;

CREATE OR REPLACE VIEW ice.ops.alert_metadata_bloat AS   -- action: expire_snapshots; write.metadata.delete-after-commit.enabled (ch9, ch10)
SELECT tbl, metadata_kb, load_kb FROM ice.ops.latest WHERE metadata_kb > 256;

CREATE OR REPLACE VIEW ice.ops.alert_manifest_sprawl AS  -- action: rewrite_manifests, after the compaction that left them (ch9)
SELECT tbl, manifests, files FROM ice.ops.latest WHERE manifests > 20;

CREATE OR REPLACE VIEW ice.ops.alert_stale_stream AS     -- a symptom, not a table action: check the writer (ch11)
SELECT tbl, freshness_min FROM ice.ops.latest WHERE class = 'stream' AND freshness_min > 5;

-- Freshness on its own schedule (per minute), one query, no pack:
-- SELECT max(committed_at) FROM ice.<ns>.<table>.snapshots;
