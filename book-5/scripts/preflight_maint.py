"""Would S3 Tables snapshot management run on this table?

Checks the conditions AWS's maintenance pages name (sources C7-26, C7-27,
C7-30) against a local table's refs and properties. Local SQLite catalog;
nothing here calls AWS.
"""
import os, shutil
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

HERE = os.path.dirname(os.path.abspath(__file__))
WH = os.path.join(HERE, "wh7")
shutil.rmtree(WH, ignore_errors=True); os.makedirs(WH)
cat = SqlCatalog("local", uri=f"sqlite:///{WH}/cat.db", warehouse=f"file://{WH}")
cat.create_namespace("bookshop")

rows = pa.table({"order_id": pa.array([1, 2, 3], pa.int64()),
                 "status": pa.array(["new", "paid", "shipped"])})

def make(name, props=None):
    t = cat.create_table(f"bookshop.{name}", rows.schema, properties=props or {})
    for _ in range(3):
        t.append(rows)
    return cat.load_table(f"bookshop.{name}")

plain = make("orders")
tagged = make("orders_tagged")
first = tagged.metadata.snapshots[0].snapshot_id
tagged.manage_snapshots().create_tag(first, "q3_close", max_ref_age_ms=3_600_000).commit()
branched = make("orders_wap")
branched.manage_snapshots().create_branch(branched.metadata.current_snapshot_id, "audit").commit()
propped = make("orders_props", {"history.expire.min-snapshots-to-keep": "1"})
nogc = make("orders_nogc", {"gc.enabled": "false"})

def check(t):
    reasons = []
    for ref, r in sorted(t.metadata.refs.items()):
        if ref != "main":
            reasons.append(f"user-defined {r.snapshot_ref_type.value} {ref!r}")
    for k in sorted(t.metadata.properties):
        if k in ("history.expire.max-snapshot-age-ms", "history.expire.min-snapshots-to-keep"):
            reasons.append(f"property {k}")
    if t.metadata.properties.get("gc.enabled", "true").lower() == "false":
        reasons.append("gc.enabled=false (also stops unreferenced file removal)")
    return reasons

for name in ["orders", "orders_tagged", "orders_wap", "orders_props", "orders_nogc"]:
    t = cat.load_table(f"bookshop.{name}")
    r = check(t)
    print(f"{name:<15} snapshots={len(t.metadata.snapshots)}  ->", "runs" if not r else "FAILS")
    for x in r:
        print("   -", x)
