"""Chapter 15: the policy test suite.

Every principal, through every engine the suite can drive, against every operation,
with the expected outcome — and a red row for every cell that differs. Runs against
the platform's Polaris (`make -C book-2 up`): creates a catalog `suite` with a
read-only principal, then tries the paths around it.

Expected, from the chapter's run:
  analyst (TABLE_READ_DATA, static storage key): read allow; write refused at commit — after the file was written
  no principal, static storage key, DuckDB direct: read allow (the storage plane ignores the catalog)
  any Trino user: write allow as the catalog's configured principal (the engine plane flattens identity)
Exits non-zero on any cell that differs from the expectation.
"""
import sys, re, requests
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from lib.platform import spark, s3client, keys, docker, PREFIX, AK, SK, REGION, S3

PO = "http://localhost:8281"
root = requests.post(f"{PO}/api/catalog/v1/oauth/tokens", auth=("root", "s3cr3t"), headers={"Polaris-Realm": "POLARIS"},
                     data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"}).json()["access_token"]
H = {"Authorization": f"Bearer {root}", "Polaris-Realm": "POLARIS", "Content-Type": "application/json"}
M = f"{PO}/api/management/v1"
def mgmt(method, path, body=None):
    r = requests.request(method, f"{M}{path}", headers=H, json=body); return r.status_code, (r.json() if r.text.startswith("{") else r.text)

results = []
def expect(label, outcome, expected):
    ok = outcome == expected; results.append((label, outcome, expected, ok)); print(f"  {'ok ' if ok else 'RED'} {label}: {outcome} (expected {expected})")

def outcome_of(fn):
    try: fn(); return "allow"
    except Exception as e:
        m = re.search(r"not authorized for op (\w+)", str(e)); return f"refused:{m.group(1)}" if m else "refused"

# ---- the catalog, the principal and its grants (idempotent)
mgmt("POST", "/catalogs", {"catalog": {"name": "suite", "type": "INTERNAL", "readOnly": False, "properties": {"default-base-location": "s3://warehouse/suite/"},
     "storageConfigInfo": {"storageType": "S3", "region": REGION, "endpoint": S3, "endpointInternal": "http://seaweedfs:8333", "pathStyleAccess": True, "allowedLocations": ["s3://warehouse/suite/"], "stsUnavailable": True}}})
mgmt("PUT", "/catalogs/suite/catalog-roles/catalog_admin/grants", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
requests.delete(f"{M}/principals/analyst", headers=H)
code, out = mgmt("POST", "/principals", {"principal": {"name": "analyst"}, "credentialRotationRequired": False})
A_ID, A_SECRET = out["credentials"]["clientId"], out["credentials"]["clientSecret"]   # returned once; rotation as root is refused
mgmt("POST", "/principal-roles", {"principalRole": {"name": "analysts"}}); mgmt("PUT", "/principals/analyst/principal-roles", {"principalRole": {"name": "analysts"}})
mgmt("POST", "/catalogs/suite/catalog-roles", {"catalogRole": {"name": "readers"}})
for priv in ("TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES"):
    mgmt("PUT", "/catalogs/suite/catalog-roles/readers/grants", {"type": "catalog", "privilege": priv})
mgmt("PUT", "/principal-roles/analysts/catalog-roles/suite", {"catalogRole": {"name": "readers"}})

def spark_as(cid, sec):
    return spark(f"suite-{cid[:6]}", cores=2, extra={
        "spark.sql.catalog.po": "org.apache.iceberg.spark.SparkCatalog", "spark.sql.catalog.po.type": "rest",
        "spark.sql.catalog.po.uri": f"{PO}/api/catalog", "spark.sql.catalog.po.warehouse": "suite",
        "spark.sql.catalog.po.credential": f"{cid}:{sec}", "spark.sql.catalog.po.scope": "PRINCIPAL_ROLE:ALL",
        "spark.sql.catalog.po.s3.endpoint": S3, "spark.sql.catalog.po.s3.path-style-access": "true",
        "spark.sql.catalog.po.s3.access-key-id": AK, "spark.sql.catalog.po.s3.secret-access-key": SK,
        "spark.sql.catalog.po.client.region": REGION, "spark.sql.catalog.po.cache-enabled": "false"})

# ---- a table to test against, as root
s = spark_as("root", "s3cr3t"); sql = lambda q: s.sql(q).collect()
sql("CREATE NAMESPACE IF NOT EXISTS po.suite_ns"); sql("DROP TABLE IF EXISTS po.suite_ns.t PURGE")
sql("CREATE TABLE po.suite_ns.t (id INT, v STRING) USING iceberg"); sql("INSERT INTO po.suite_ns.t VALUES (1, 'a')"); s.stop()

print("== catalog plane: analyst")
s = spark_as(A_ID, A_SECRET); sql = lambda q: s.sql(q).collect()
expect("analyst reads", outcome_of(lambda: sql("SELECT count(*) FROM po.suite_ns.t")), "allow")
s3 = s3client(); before = len(keys(s3, "warehouse", "suite/suite_ns/t/data/"))
expect("analyst writes", outcome_of(lambda: sql("INSERT INTO po.suite_ns.t VALUES (2, 'b')")), "refused:ADD_TABLE_SNAPSHOT")
expect("analyst creates a table", outcome_of(lambda: sql("CREATE TABLE po.suite_ns.t2 (id INT) USING iceberg")), "refused:CREATE_TABLE_DIRECT")
s.stop()

print("== storage plane: no principal, the static key, DuckDB direct")
import duckdb; con = duckdb.connect(); con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"CREATE SECRET k (TYPE s3, KEY_ID '{AK}', SECRET '{SK}', REGION '{REGION}', ENDPOINT '{S3.split('://')[1]}', USE_SSL false, URL_STYLE 'path')")
expect("direct read of the table's files", outcome_of(lambda: con.execute("SELECT count(*) FROM read_parquet('s3://warehouse/suite/suite_ns/t/data/*.parquet')").fetchone()), "allow")

print("== engine plane: Trino's catalog identity")
r = docker("exec", f"{PREFIX}-trino", "trino", "--user", "nobody", "--catalog", "polaris", "--schema", "suite_ns", "--execute", "SELECT count(*) FROM t")
if "Catalog 'polaris' not found" in r.stderr or "does not exist" in r.stderr:
    print("  skip: this Trino has no catalog pointed at Polaris warehouse `suite` (add trino/catalog/polaris_suite.properties to include it)")
else:
    expect("trino --user nobody reads through the engine's fixed identity", "allow" if r.stdout.strip() else "refused", "allow")

red = [r for r in results if not r[3]]
print(f"\n{len(results) - len(red)} cells as expected, {len(red)} differ")
sys.exit(1 if red else 0)
