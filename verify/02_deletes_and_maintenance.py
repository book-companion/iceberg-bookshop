"""Chapters 10-13: copy-on-write vs merge-on-read, then wreck and repair.

Needs the catalog up (`make up`) and Spark. First run downloads the Iceberg
runtime jar from Maven Central (~47 MB).
"""
import time
from bookshop.spark import spark_session

spark = spark_session("deletes-and-maintenance")
q = spark.sql
NS = "ice.bookshop"

def counts(t):
    d = q(f"SELECT count(*) n FROM {NS}.{t}.data_files").collect()[0].n
    x = q(f"SELECT count(*) n FROM {NS}.{t}.delete_files").collect()[0].n
    return d, x

print("== copy-on-write vs merge-on-read (ch10) ==")
print("   NOTE: .files is the UNION of data and delete files. Use .data_files")
print("   and .delete_files, or you will miscount a merge-on-read table.")
for mode in ("copy-on-write", "merge-on-read"):
    t = "cow" if mode.startswith("copy") else "mor"
    q(f"DROP TABLE IF EXISTS {NS}.{t}")
    q(f"""CREATE TABLE {NS}.{t} (order_id BIGINT, status STRING, amount DOUBLE)
          USING iceberg TBLPROPERTIES ('write.delete.mode'='{mode}',
          'write.update.mode'='{mode}','write.merge.mode'='{mode}')""")
    q(f"INSERT INTO {NS}.{t} SELECT id,'placed',id*1.0 FROM range(1000)")
    for i in (7, 100, 101, 102, 103, 104):
        q(f"DELETE FROM {NS}.{t} WHERE order_id = {i}")
    d, x = counts(t)
    recs = q(f"SELECT coalesce(sum(record_count),0) r FROM {NS}.{t}.data_files").collect()[0].r
    print(f"  {mode:14} data_files={d} (holding {recs} records)  delete_files={x}"
          f"  rows={q(f'SELECT count(*) n FROM {NS}.{t}').collect()[0].n}")

print("\n== wreck it, then repair it (ch11-13) ==")
T = "wrecked"
q(f"DROP TABLE IF EXISTS {NS}.{T}")
q(f"CREATE TABLE {NS}.{T} (order_id BIGINT, country STRING, amount DOUBLE) USING iceberg")
q(f"INSERT INTO {NS}.{T} SELECT id, CASE WHEN id%3=0 THEN 'GB' WHEN id%3=1 THEN 'US' ELSE 'DE' END, id*1.0 FROM range(20000)")
for i in range(30):
    q(f"INSERT INTO {NS}.{T} VALUES ({900000+i},'GB',9.99)")

def timed():
    best = 9e9
    for _ in range(3):
        s = time.time(); q(f"SELECT country, count(*), sum(amount) FROM {NS}.{T} GROUP BY country").collect()
        best = min(best, time.time() - s)
    return best * 1000

before_files, _ = counts(T); before_ms = timed()
print(f"  wrecked   : data_files={before_files}  snapshots="
      f"{q(f'SELECT count(*) n FROM {NS}.{T}.snapshots').collect()[0].n}  query {before_ms:.1f} ms")
r = q(f"CALL ice.system.rewrite_data_files(table=>'bookshop.{T}', options=>map('min-input-files','2'))").collect()[0]
print("  compact   :", {k: v for k, v in r.asDict().items() if "count" in k})
e = q(f"CALL ice.system.expire_snapshots(table=>'bookshop.{T}', older_than=>TIMESTAMP '2099-01-01 00:00:00', retain_last=>1)").collect()[0]
print("  expire    :", {k: v for k, v in e.asDict().items() if v})
after_files, _ = counts(T); after_ms = timed()
print(f"  repaired  : data_files={after_files}  snapshots="
      f"{q(f'SELECT count(*) n FROM {NS}.{T}.snapshots').collect()[0].n}  query {after_ms:.1f} ms "
      f"({before_ms/after_ms:.1f}x)")
assert after_files < before_files, "compaction should reduce the file count"
print("\nOK")
spark.stop()
