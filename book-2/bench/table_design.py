"""Chapter 6 — workload-aware table design. Run with the platform up
(`make -C book-2 up`), from the repository root: ../.venv/bin/python book-2/bench/table_design.py
 Three bookshop workloads, each
built two ways, measured under the book's protocol:

  pre-warm both arms, INTERLEAVE the arms across rounds, report the MEDIAN
  and the spread. Book 1's retracted 2.3x (JVM warm-up measured as a write
  mode) is the reason.

Latency comes from wall clocks; files and bytes come from the counting proxy
in front of the object store (every S3 GET a query makes, and how many bytes
it fetched), which does not care about warm-up at all.

  W1  append-only events   A: unpartitioned, unsorted
                           B: partitioned by day, sorted by customer_id
      Q: one day's revenue; one customer's events
  W2  upsert-heavy orders  A: copy-on-write MERGE      B: merge-on-read MERGE
      5 rounds of MERGE touching 1% of rows; read cost after; delete files
  W3  wide dimension (60 cols)  A: unsorted   B: sorted by customer_id
      Q: point lookup by customer_id; a 3-column projection over everything
"""
import os, re, time, statistics, subprocess, collections
os.environ.pop("SPARK_HOME", None)
LAB = os.path.dirname(os.path.abspath(__file__))
ENV = dict(os.environ)
AK, SK, REGION = "lab-access-key", "lab-secret-key", "us-east-1"
PROXY, REST = "http://localhost:8334", "http://localhost:8181"
os.environ.update(AWS_ACCESS_KEY_ID=AK, AWS_SECRET_ACCESS_KEY=SK, AWS_REGION=REGION)

def log_lines():
    return subprocess.run(["docker", "exec", "book2-s3-counter", "cat", "/var/log/nginx/s3.log"], capture_output=True, text=True, env=ENV).stdout.splitlines()
def classify(line):
    m = re.match(r"(\w+) (\S+) (\d+) (\d+) ", line)
    if not m: return None
    method, uri, status, nbytes = m.groups(); path = uri.split("?", 1)[0]
    kind = ("GET data" if method == "GET" and "/data/" in path else "GET metadata" if method == "GET" else method)
    return kind, int(nbytes)
def counted(fn):
    """run fn once; return (result, {'GET data': n, ...}, data_bytes)"""
    before = len(log_lines()); out = fn(); new = log_lines()[before:]
    c = collections.Counter(); b = 0
    for l in new:
        k = classify(l)
        if k:
            c[k[0]] += 1
            if k[0] == "GET data": b += k[1]
    return out, dict(c), b
def bench(label, arms, rounds=7, warm=2):
    """arms: {name: fn}. pre-warm each, then interleave. prints medians + spread."""
    for name, fn in arms.items():
        for _ in range(warm): fn()
    times = {n: [] for n in arms}
    for _ in range(rounds):
        for name, fn in arms.items():
            t = time.perf_counter(); fn(); times[name].append((time.perf_counter() - t) * 1000)
    out = []
    for name, ts in times.items():
        out.append(f"{name}: median {statistics.median(ts):7.1f} ms  (min {min(ts):.0f}, max {max(ts):.0f})")
    print(f"  {label:<44} " + "   ".join(out))
    return {n: statistics.median(ts) for n, ts in times.items()}

from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]").appName("stage9").config("spark.ui.enabled", "false")
    .config("spark.driver.memory", "4g")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.ice.type", "rest")
    .config("spark.sql.catalog.ice.uri", REST).config("spark.sql.catalog.ice.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.ice.s3.endpoint", PROXY).config("spark.sql.catalog.ice.s3.path-style-access", "true")
    .config("spark.sql.catalog.ice.client.region", REGION).config("spark.sql.catalog.ice.cache-enabled", "false")
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()); spark.sparkContext.setLogLevel("ERROR")
sql = lambda s: spark.sql(s).collect()
def files_of(t):
    r = sql(f"SELECT count(*), sum(file_size_in_bytes) FROM ice.stage9.{t}.files")[0]; return r[0], (r[1] or 0)
sql("CREATE NAMESPACE IF NOT EXISTS ice.stage9")

# ================================================================ W1: append-only events
print("== W1 append-only events: 2,000,000 rows over 9 days, 500 customers")
EVENTS = """SELECT id AS event_id, CAST(id % 500 AS INT) AS customer_id, CAST(10 + (id % 900) / 7.0 AS DECIMAL(10,2)) AS amount,
  timestamp'2026-06-01 00:00:00' + make_interval(0,0,0,CAST(id % 9 AS INT),0,0,CAST((id * 7) % 86400 AS INT)) AS ts FROM range(2000000)"""
sql("DROP TABLE IF EXISTS ice.stage9.events_a"); sql("DROP TABLE IF EXISTS ice.stage9.events_b")
sql(f"CREATE TABLE ice.stage9.events_a (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP) USING iceberg")
sql(f"CREATE TABLE ice.stage9.events_b (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts))")
sql("ALTER TABLE ice.stage9.events_b WRITE ORDERED BY customer_id")
t = time.perf_counter(); sql(f"INSERT INTO ice.stage9.events_a {EVENTS}"); ta = time.perf_counter() - t
t = time.perf_counter(); sql(f"INSERT INTO ice.stage9.events_b {EVENTS}"); tb = time.perf_counter() - t
fa, ba = files_of("events_a"); fb, bb = files_of("events_b")
print(f"  A unpartitioned/unsorted: {fa} files, {ba/1e6:.1f} MB, written in {ta:.1f}s   B day-partitioned + sorted by customer: {fb} files, {bb/1e6:.1f} MB, written in {tb:.1f}s")
Q_DAY = "SELECT sum(amount) FROM ice.stage9.{t} WHERE ts >= '2026-06-03' AND ts < '2026-06-04'"
Q_CUST = "SELECT count(*), sum(amount) FROM ice.stage9.{t} WHERE customer_id = 123"
for q, label in ((Q_DAY, "one day's revenue"), (Q_CUST, "one customer's events")):
    for arm in ("events_a", "events_b"):
        _, c, b = counted(lambda: sql(q.format(t=arm)))
        print(f"  {label:<24} {arm}: data GETs {c.get('GET data',0):>3}, data bytes {b/1e6:6.1f} MB")
    bench(label, {"A": lambda q=q: sql(q.format(t="events_a")), "B": lambda q=q: sql(q.format(t="events_b"))})

# ================================================================ W2: upsert-heavy orders
print("== W2 upsert-heavy orders: 1,000,000 rows; 5 rounds of MERGE touching 1% of rows")
for arm, mode in (("orders_cow", "copy-on-write"), ("orders_mor", "merge-on-read")):
    sql(f"DROP TABLE IF EXISTS ice.stage9.{arm}")
    sql(f"""CREATE TABLE ice.stage9.{arm} (order_id BIGINT, customer_id INT, status STRING, amount DECIMAL(10,2), updated_at TIMESTAMP) USING iceberg
            PARTITIONED BY (bucket(8, order_id)) TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='{mode}', 'write.update.mode'='{mode}', 'write.delete.mode'='{mode}')""")
    sql(f"INSERT INTO ice.stage9.{arm} SELECT id, CAST(id % 5000 AS INT), 'placed', CAST(id % 100 AS DECIMAL(10,2)), timestamp'2026-06-01 00:00:00' FROM range(1000000)")
def merge(arm, rnd):
    sql(f"""MERGE INTO ice.stage9.{arm} t USING (SELECT id AS order_id FROM range(1000000) WHERE id % 100 = {rnd}) s
            ON t.order_id = s.order_id WHEN MATCHED THEN UPDATE SET status = 'shipped', updated_at = timestamp'2026-06-02 00:00:00'""")
# protocol: pre-warm with round 0 on each, then interleave rounds 1..5
merge("orders_cow", 0); merge("orders_mor", 0)
mt = {"cow": [], "mor": []}
for rnd in range(1, 6):
    t = time.perf_counter(); merge("orders_cow", rnd); mt["cow"].append((time.perf_counter() - t) * 1000)
    t = time.perf_counter(); merge("orders_mor", rnd); mt["mor"].append((time.perf_counter() - t) * 1000)
print(f"  MERGE 1% (5 interleaved rounds)              cow: median {statistics.median(mt['cow']):7.0f} ms (min {min(mt['cow']):.0f}, max {max(mt['cow']):.0f})   mor: median {statistics.median(mt['mor']):7.0f} ms (min {min(mt['mor']):.0f}, max {max(mt['mor']):.0f})")
for arm in ("orders_cow", "orders_mor"):
    d = sql(f"SELECT count(*) FROM ice.stage9.{arm}.data_files")[0][0]; dl = sql(f"SELECT count(*), sum(record_count) FROM ice.stage9.{arm}.delete_files")[0]
    print(f"  {arm}: {d} data files, {dl[0]} delete files holding {dl[1] or 0} deleted positions")
Q_READ = "SELECT status, count(*) FROM ice.stage9.{t} GROUP BY status"
for arm in ("orders_cow", "orders_mor"):
    _, c, b = counted(lambda: sql(Q_READ.format(t=arm))); print(f"  read after 6 merges  {arm}: data GETs {c.get('GET data',0)}, data bytes {b/1e6:.1f} MB")
bench("read: status counts, after 6 merges", {"cow": lambda: sql(Q_READ.format(t="orders_cow")), "mor": lambda: sql(Q_READ.format(t="orders_mor"))})
t = time.perf_counter(); r = sql("CALL ice.system.rewrite_position_delete_files(table => 'stage9.orders_mor')")[0]; tp = time.perf_counter() - t
t = time.perf_counter(); r2 = sql("CALL ice.system.rewrite_data_files(table => 'stage9.orders_mor', options => map('min-input-files','2'))")[0]; tc = time.perf_counter() - t
print(f"  mor maintenance: rewrite_position_delete_files {tp:.1f}s -> {r[:2]}; rewrite_data_files {tc:.1f}s -> {r2[:2]}")
dl = sql("SELECT count(*) FROM ice.stage9.orders_mor.delete_files")[0][0]; df = sql("SELECT count(*) FROM ice.stage9.orders_mor.data_files")[0][0]
_, c, b = counted(lambda: sql(Q_READ.format(t="orders_mor")))
print(f"  mor after compaction: {df} data files, {dl} delete files still listed (Book 1 ch12: compaction leaves them), read now {c.get('GET data',0)} data GETs")
bench("read: status counts, mor after compaction", {"cow": lambda: sql(Q_READ.format(t="orders_cow")), "mor": lambda: sql(Q_READ.format(t="orders_mor"))})

# ================================================================ W3: wide dimension
print("== W3 wide dimension: 1,000,000 customers x 60 columns")
cols = ", ".join(f"CAST(id * {i} % 1000 AS INT) AS attr_{i:02d}" for i in range(1, 58))
DIM = f"SELECT id AS customer_id, concat('customer-', id) AS name, 'DE' AS country, {cols} FROM range(1000000)"
for arm in ("dim_a", "dim_b"):
    sql(f"DROP TABLE IF EXISTS ice.stage9.{arm}")
    sql(f"CREATE TABLE ice.stage9.{arm} USING iceberg AS {DIM} WHERE 1 = 0")
    if arm == "dim_b": sql("ALTER TABLE ice.stage9.dim_b WRITE ORDERED BY customer_id")
    # write in 8 batches so the unsorted table has several files with overlapping id ranges
    for k in range(8): sql(f"INSERT INTO ice.stage9.{arm} {DIM.replace('FROM range(1000000)', f'FROM range(1000000) WHERE id % 8 = {k}')}")
    f, b = files_of(arm); print(f"  {arm}: {f} files, {b/1e6:.1f} MB")
Q_POINT = "SELECT name, attr_07 FROM ice.stage9.{t} WHERE customer_id = 424242"
Q_PROJ = "SELECT country, count(*), avg(attr_07) FROM ice.stage9.{t} GROUP BY country"
Q_ALL = "SELECT count(*) FROM ice.stage9.{t} WHERE attr_31 = 7 AND attr_44 = 3"
for q, label in ((Q_POINT, "point lookup by customer_id"), (Q_PROJ, "3-column projection, all rows"), (Q_ALL, "2-column filter, all rows")):
    for arm in ("dim_a", "dim_b"):
        _, c, b = counted(lambda: sql(q.format(t=arm)))
        print(f"  {label:<30} {arm}: data GETs {c.get('GET data',0):>3}, data bytes {b/1e6:6.1f} MB")
    bench(label, {"A": lambda q=q: sql(q.format(t="dim_a")), "B": lambda q=q: sql(q.format(t="dim_b"))})
# FIRST RUN: A and B were identical on every query — 32 data GETs for a point
# lookup on both. WRITE ORDERED BY sorts within each write, and each of the
# eight writes spans the whole customer_id range, so every file's min/max
# brackets every id and nothing prunes. Clustering has to hold ACROSS files:
# a sort-strategy compaction rewrites the table into files with disjoint ranges.
print("  -- rewrite dim_b with strategy => 'sort' on customer_id (clustering across files) --")
t = time.perf_counter(); r = sql("CALL ice.system.rewrite_data_files(table => 'stage9.dim_b', strategy => 'sort', sort_order => 'customer_id ASC NULLS LAST', options => map('min-input-files','2'))")[0]; tc = time.perf_counter() - t   # default target size; a 2 MB target in the first run fragmented B into 86 files and made the full scan 2.5x slower
f, b = files_of("dim_b"); print(f"  dim_b after sort compaction: rewrote {r[0]} files into {r[1]} in {tc:.1f}s; now {f} files, {b/1e6:.1f} MB")
for q, label in ((Q_POINT, "point lookup, B clustered"), (Q_ALL, "2-column filter, B clustered")):
    for arm in ("dim_a", "dim_b"):
        _, c, bb = counted(lambda: sql(q.format(t=arm)))
        print(f"  {label:<30} {arm}: data GETs {c.get('GET data',0):>3}, data bytes {bb/1e6:6.1f} MB")
    bench(label, {"A": lambda q=q: sql(q.format(t="dim_a")), "B": lambda q=q: sql(q.format(t="dim_b"))})
spark.stop(); print("STAGE 9 DONE")
