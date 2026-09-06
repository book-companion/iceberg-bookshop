"""Chapters 1-4: the four-file rule, statistics pruning, and the catalog's five columns.

Runs entirely on a local SQLite catalog. No Docker, no JVM.
"""
import glob, os, sqlite3
from bookshop.catalog import local_catalog, ROOT
from bookshop.schema import ORDERS
from bookshop.seed import orders_batch

BASE = ROOT / "labs/anatomy"
cat = local_catalog("anatomy", "labs/anatomy")
cat.create_namespace_if_not_exists("bookshop")
if ("bookshop", "orders") in cat.list_tables("bookshop"):
    cat.drop_table("bookshop.orders")
t = cat.create_table("bookshop.orders", schema=ORDERS)

def files():
    fs = [f for f in glob.glob(str(BASE / "wh/**/*"), recursive=True) if os.path.isfile(f)]
    k = {}
    for f in fs:
        b = os.path.basename(f)
        n = ("data" if b.endswith(".parquet") else "metadata.json" if b.endswith(".metadata.json")
             else "manifest list" if b.startswith("snap-") else "manifest" if b.endswith(".avro") else "other")
        k[n] = k.get(n, 0) + 1
    return len(fs), k

print("== every commit adds exactly four files (ch3) ==")
print(f"  after create_table  total {files()[0]:3}  {files()[1]}")
for i, status in enumerate(["placed", "shipped", "placed"]):
    t.append(orders_batch(1 + i * 100, 100, ORDERS, status=status))
    print(f"  after append {i+1}      total {files()[0]:3}  {files()[1]}")

print("\n== statistics prune files before they are opened (ch3) ==")
for f in ["order_id >= 1", "order_id > 250", "order_id > 500", "status == 'shipped'"]:
    n = len(list(t.scan(row_filter=f).plan_files()))
    print(f"  {f:22} -> {n} of 3 files planned, {t.scan(row_filter=f).to_arrow().num_rows} rows")

print("\n== the catalog is five columns (ch4) ==")
con = sqlite3.connect(BASE / "catalog.db")
cols = [r[1] for r in con.execute("PRAGMA table_info(iceberg_tables)")]
print("  iceberg_tables:", cols)
row = con.execute("select metadata_location from iceberg_tables").fetchone()[0]
print("  pointer before:", os.path.basename(row))
t.append(orders_batch(9001, 10, ORDERS, status="placed"))
row2 = sqlite3.connect(BASE / "catalog.db").execute("select metadata_location from iceberg_tables").fetchone()[0]
print("  pointer after :", os.path.basename(row2))
assert row != row2, "the pointer should move on commit"
print("\nOK")
