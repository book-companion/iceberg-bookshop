-- Chapter 7: the skew diagnosis, from metadata only. Replace ice.<ns>.<table>.
-- Runs in any engine that exposes the metadata tables; reads no data files.

-- Value skew, and the small-file ratio per partition.
SELECT partition, record_count, file_count,
       ROUND(total_data_file_size_in_bytes / 1048576.0, 2)              AS mb,
       ROUND(total_data_file_size_in_bytes / file_count / 1048576.0, 2) AS mb_per_file
FROM   ice.<ns>.<table>.partitions
ORDER  BY record_count DESC;

-- The file-size distribution: how much of the table is small files.
SELECT count(*)                                                        AS files,
       ROUND(percentile_approx(file_size_in_bytes, 0.5) / 1024.0)     AS p50_kb,
       ROUND(max(file_size_in_bytes) / 1048576.0, 2)                  AS max_mb,
       sum(CASE WHEN file_size_in_bytes < 33554432 THEN 1 ELSE 0 END) AS under_32mb
FROM   ice.<ns>.<table>.files;

-- Which spec each file was written under: a table that spans two spec ids has
-- been evolved (ALTER TABLE ... ADD PARTITION FIELD) and not yet rewritten.
SELECT spec_id, count(*) AS files FROM ice.<ns>.<table>.files GROUP BY spec_id;

-- Predicate skew is not in the metadata. It comes from the query log; chapter 13.
