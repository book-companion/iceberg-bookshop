-- Chapter 8: what a default rewrite_data_files will pick, from metadata, before it runs.
-- A candidate is a file under 75% of the target, grouped by partition, in a group of at
-- least min-input-files (5). Substitute the table's write.target-file-size-bytes for
-- 536870912 if it is set. The prediction matched the plan exactly in the book's run.
SELECT partition, count(*) AS candidates
FROM   ice.<ns>.<table>.files
WHERE  content = 0                              -- data files only; .files also lists delete files
  AND  file_size_in_bytes < 0.75 * 536870912
GROUP  BY partition
HAVING count(*) >= 5;

-- A file with a third of its rows deleted is a candidate at any size (delete-ratio-threshold 0.3).
-- The size query above cannot see that; the delete_files table can:
SELECT d.file_path AS delete_file, d.record_count AS deleted_rows
FROM   ice.<ns>.<table>.delete_files d
ORDER  BY deleted_rows DESC;
