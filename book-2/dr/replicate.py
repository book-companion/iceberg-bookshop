"""Chapter 16: bucket replication is not table replication.

Copies one table's objects from the primary store to a replica, in the order you
choose, and then validates the replica: register its newest metadata in a scratch
catalog and list the files that metadata references against what the replica holds.

  python dr/replicate.py <namespace> <table> [--order metadata-first|data-first] [--replica http://localhost:8335] [--commit-between N]

--commit-between N appends N small commits to the table between the two halves of the
copy (through Spark), which is the race a live writer produces; without it, a quiet
table replicates consistently in either order.

Under a live writer, data-first leaves a replica whose newest metadata names files
the replica does not have (the book's run: 4 of 8 missing, scan fails). Metadata-first
leaves a consistent, slightly older table. The recovery point of a lakehouse is the
age of its newest CONSISTENT metadata file.
"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.platform import s3client, keys, AK, SK, REGION
from pyiceberg.catalog import load_catalog

ns, t = sys.argv[1], sys.argv[2]
order = sys.argv[sys.argv.index("--order") + 1] if "--order" in sys.argv else "metadata-first"
replica = sys.argv[sys.argv.index("--replica") + 1] if "--replica" in sys.argv else "http://localhost:8335"
pri, dr = s3client(), s3client(replica)
try: dr.create_bucket(Bucket="warehouse")
except Exception: pass
commits = int(sys.argv[sys.argv.index("--commit-between") + 1]) if "--commit-between" in sys.argv else 0
halves = ("metadata/", "data/") if order == "metadata-first" else ("data/", "metadata/")
t0 = time.perf_counter(); n = 0
for i, sub in enumerate(halves):
    for k in [k for k in keys(pri, "warehouse", f"{ns}/{t}/") if f"/{sub}" in k]:
        dr.put_object(Bucket="warehouse", Key=k, Body=pri.get_object(Bucket="warehouse", Key=k)["Body"].read()); n += 1
    if i == 0 and commits:
        from lib.platform import spark
        s = spark("replicate-writer", cores=2)
        cols = [f.name for f in s.table(f"ice.{ns}.{t}").schema.fields]
        for c in range(commits):
            s.sql(f"INSERT INTO ice.{ns}.{t} SELECT * FROM ice.{ns}.{t} LIMIT 10")
        s.stop(); print(f"{commits} commits landed between the halves")
print(f"copied {n} objects, {order}, in {time.perf_counter()-t0:.1f}s")
cat = load_catalog("dr", **{"type": "sql", "uri": f"sqlite:///{os.path.dirname(__file__)}/dr-catalog.db", "warehouse": "s3://warehouse/",
                             "s3.endpoint": replica, "s3.access-key-id": AK, "s3.secret-access-key": SK, "s3.region": REGION})
try: cat.create_namespace(ns)
except Exception: pass
try: cat.drop_table(f"{ns}.{t}")
except Exception: pass
metas = sorted(k for k in keys(dr, "warehouse", f"{ns}/{t}/metadata/") if k.endswith(".metadata.json"))
tbl = cat.register_table(f"{ns}.{t}", f"s3://warehouse/{metas[-1]}")
have = set(keys(dr, "warehouse", f"{ns}/{t}/")); want = [f["file_path"].split("/warehouse/", 1)[1] for f in tbl.inspect.files().to_pylist()]
missing = [w for w in want if w not in have]
print(f"replica's newest metadata: {len(tbl.metadata.snapshots)} snapshots, references {len(want)} data files, {len(missing)} MISSING")
try: print(f"scan -> {len(tbl.scan().to_arrow())} rows")
except Exception as e: print(f"scan -> FAILED: {str(e)[:120]}")
sys.exit(1 if missing else 0)
