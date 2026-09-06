"""Chapter 1: the platform is up, and every engine agrees on one table.

Needs `make -C book-2 up`. Prints the version manifest the chapter prints,
then proves the platform end to end: Spark writes 50,000 orders through the
reference REST catalog onto object storage, PyIceberg and DuckDB each commit
one row through the same catalog, Trino reads the result, and every engine
reports the same count. Asserts its own results.

Every host client sets its own s3.endpoint: the catalog was configured with
http://seaweedfs:8333, a name only containers resolve.
"""
import json, os, subprocess, sys, time
from decimal import Decimal
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
S3, REST = "http://localhost:8333", "http://localhost:8181"
AK, SK, REGION = "lab-access-key", "lab-secret-key", "us-east-1"
os.environ.update(AWS_ACCESS_KEY_ID=AK, AWS_SECRET_ACCESS_KEY=SK, AWS_REGION=REGION)

def sh(*a):
    r = subprocess.run(a, capture_output=True, text=True)
    if r.stderr.strip():
        print("   [stderr]", r.stderr.strip().replace("\n", " ")[:300], file=sys.stderr)
    return r.stdout.strip()

# ---------------------------------------------------------------- manifest
print("== version manifest")
for name, cmd in [("trino", ["docker", "exec", "book2-trino", "trino", "--output-format=CSV_UNQUOTED", "--execute", "SELECT version()"]),
                  ("flink", ["curl", "-s", "http://localhost:8081/config"]),
                  ("lakekeeper", ["curl", "-s", "http://localhost:8381/management/v1/info"]),
                  ("kafka connect", ["curl", "-s", "http://localhost:8083/"]),
                  ("postgres", ["docker", "exec", "book2-postgres", "psql", "-U", "postgres", "-At", "-c", "SHOW server_version"])]:
    out = sh(*cmd)
    if out.startswith("{"):
        d = json.loads(out); out = d.get("flink-version") or d.get("version") or out[:40]
    print(f"   {name:<14} {out[:40]}")
import pyspark, pyiceberg, duckdb
print(f"   {'pyspark':<14} {pyspark.__version__}\n   {'pyiceberg':<14} {pyiceberg.__version__}\n   {'duckdb':<14} {duckdb.__version__}")

# ---------------------------------------------------------------- Spark writes
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]").appName("book2-platform")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "rest").config("spark.sql.catalog.ice.uri", REST)
    .config("spark.sql.catalog.ice.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.ice.s3.endpoint", S3).config("spark.sql.catalog.ice.s3.path-style-access", "true")
    .config("spark.sql.catalog.ice.client.region", REGION).config("spark.ui.enabled", "false")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.platform")
spark.sql("DROP TABLE IF EXISTS ice.platform.orders")
spark.sql("""CREATE TABLE ice.platform.orders (order_id BIGINT, customer_id INT, country STRING,
             amount DECIMAL(10,2), ordered_at TIMESTAMP) USING iceberg PARTITIONED BY (days(ordered_at))""")
t0 = time.perf_counter()
spark.sql("""INSERT INTO ice.platform.orders
  SELECT id, CAST(id % 500 AS INT), element_at(array('DE','GB','US'), CAST(id % 3 AS INT) + 1),
         CAST(10 + (id % 900) / 7.0 AS DECIMAL(10,2)),
         timestamp'2026-06-01 00:00:00' + make_interval(0,0,0,CAST(id % 9 AS INT),0,0,0)
  FROM range(50000)""")
files = spark.sql("SELECT file_path FROM ice.platform.orders.files").collect()
assert all(f.file_path.startswith("s3://warehouse/") for f in files), files[:1]
print(f"== spark wrote 50,000 rows into {len(files)} files under s3://warehouse/ in {time.perf_counter()-t0:.1f}s")

# ---------------------------------------------------------------- PyIceberg commits
from pyiceberg.catalog import load_catalog
import pyarrow as pa
cat = load_catalog("rest", **{"uri": REST, "s3.endpoint": S3, "s3.access-key-id": AK, "s3.secret-access-key": SK, "s3.region": REGION})
tbl = cat.load_table("platform.orders")
tbl.append(pa.Table.from_pylist([{"order_id": 50000, "customer_id": 1, "country": "DE", "amount": Decimal("99.99"),
                                  "ordered_at": datetime(2026, 6, 10, 12, 0, 0)}], schema=tbl.schema().as_arrow()))
print("== pyiceberg appended 1 row through the catalog")

# ---------------------------------------------------------------- DuckDB commits
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
con.execute(f"CREATE SECRET s3 (TYPE s3, KEY_ID '{AK}', SECRET '{SK}', REGION '{REGION}', ENDPOINT 'localhost:8333', USE_SSL false, URL_STYLE 'path')")
con.execute(f"ATTACH '' AS ice (TYPE iceberg, ENDPOINT '{REST}', AUTHORIZATION_TYPE 'none')")
con.execute("INSERT INTO ice.platform.orders VALUES (50001, 2, 'GB', 12.50, TIMESTAMP '2026-06-11 09:00:00')")
print("== duckdb inserted 1 row through the catalog")

# ---------------------------------------------------------------- everybody counts
stale = spark.sql("SELECT count(*) FROM ice.platform.orders").collect()[0][0]
spark.sql("REFRESH TABLE ice.platform.orders")
counts = {
    "spark": spark.sql("SELECT count(*) FROM ice.platform.orders").collect()[0][0],
    "pyiceberg": len(cat.load_table("platform.orders").scan().to_arrow()),
    "duckdb": con.execute("SELECT count(*) FROM ice.platform.orders").fetchone()[0],
    "trino": int(sh("docker", "exec", "book2-trino", "trino", "--output-format=CSV_UNQUOTED", "--execute",
                    "SELECT count(*) FROM fixture.platform.orders") or 0),
}
print(f"== spark saw {stale} before REFRESH TABLE (its REST client caches table metadata)")
print("== counts:", counts)
assert all(v == 50002 for v in counts.values()), counts
snaps = len(cat.load_table("platform.orders").metadata.snapshots)
assert snaps == 3, snaps
print(f"== PASS: four engines, one catalog, one table, 50,002 rows, {snaps} snapshots")
spark.stop()
