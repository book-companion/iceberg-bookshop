"""Chapter 15: three engines, one table, one catalog, the same number.

Needs the catalog up (`make up`) and the tables seeded (`make seed`).
No JVM: this is PyIceberg and DuckDB only.
"""
import duckdb
from bookshop.catalog import rest_catalog, duckdb_attached

QUESTION = "revenue by country"

cat = rest_catalog()
t = cat.load_table("bookshop.orders")
arrow = t.scan().to_arrow()

pyi = (
    duckdb.arrow(arrow)
    .aggregate("country, count(*) n, round(sum(amount),2) rev")
    .order("country")
    .fetchall()
)
print(f"  PyIceberg -> Arrow : {arrow.num_rows} rows | {pyi}")

con = duckdb_attached()
duck = con.sql(
    "SELECT country, count(*) n, round(sum(amount),2) rev "
    "FROM ice.bookshop.orders GROUP BY country ORDER BY country"
).fetchall()
duck_rows = con.sql("SELECT count(*) FROM ice.bookshop.orders").fetchone()[0]
print(f"  DuckDB via catalog : {duck_rows} rows | {duck}")

assert pyi == duck, "engines disagree"
assert arrow.num_rows == duck_rows, "row counts disagree"
print(f"\n  Both engines agree on {QUESTION}, to the cent.")
print("  format_version:", t.format_version, "| data files:", t.inspect.files().num_rows)
print("\nOK")
