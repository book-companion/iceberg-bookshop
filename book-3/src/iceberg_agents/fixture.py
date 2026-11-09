"""Authored input, never captured output. Seed a real local Iceberg table."""
from __future__ import annotations

from datetime import date
from pathlib import Path

ROWS = [
    (41001, "north", 101, "2026-09-01", "paid", "USD", 10000, 1000, 0),
    (41002, "north", 102, "2026-09-01", "paid", "USD", 5000, 0, 1000),
    (41003, "north", 103, "2026-09-01", "cancelled", "USD", 2000, 0, 0),
    (41004, "south", 201, "2026-09-01", "paid", "USD", 8000, 0, 0),
    (41005, "south", 202, "2026-09-02", "paid", "USD", 12000, 2000, 0),
    (41006, "north", 104, "2026-09-02", "paid", "USD", 3500, 500, 0),
    (41007, "south", 201, "2026-09-02", "pending", "USD", 5000, 0, 0),
    (41008, "north", 101, "2026-09-02", "paid", "USD", 7000, 0, 2000),
]
COLUMNS = ("order_id", "tenant", "customer_id", "order_date", "status", "currency", "gross_cents", "discount_cents", "refund_cents")


def records() -> list[dict]:
    result = [dict(zip(COLUMNS, row, strict=True)) for row in ROWS]
    for row in result:
        row["order_date"] = date.fromisoformat(row["order_date"])
    return result


def arrow_schema():
    import pyarrow as pa
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("tenant", pa.string(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_date", pa.date32(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("gross_cents", pa.int64(), nullable=False),
        pa.field("discount_cents", pa.int64(), nullable=False),
        pa.field("refund_cents", pa.int64(), nullable=False),
    ])


def local_catalog(work: Path):
    from pyiceberg.catalog import load_catalog
    work = work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    return load_catalog("book3_local", type="sql", uri=f"sqlite:///{work / 'catalog.db'}", warehouse=(work / "warehouse").as_uri())


def seed(work: Path, identifier: str = "agents_demo.orders") -> dict:
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, LongType, StringType, DateType
    catalog = local_catalog(work)
    namespace, _ = identifier.split(".")
    if not namespace.startswith("agents_"):
        raise ValueError("Only agents_* namespaces belong to this companion")
    catalog.create_namespace_if_not_exists(namespace)
    if catalog.table_exists(identifier):
        raise ValueError("Table already exists; select a fresh --work directory for an independent experiment")
    types = [LongType(), StringType(), LongType(), DateType(), StringType(), StringType(), LongType(), LongType(), LongType()]
    schema = Schema(*(NestedField(i, name, typ, required=True) for i, (name, typ) in enumerate(zip(COLUMNS, types, strict=True), 1)))
    table = catalog.create_table(identifier, schema=schema, properties={"format-version": "2"})
    table.append(pa.Table.from_pylist(records(), schema=arrow_schema()))
    snap = table.current_snapshot()
    return {"table": identifier, "snapshot_id": snap.snapshot_id, "rows": len(ROWS), "metadata_location": table.metadata_location}
