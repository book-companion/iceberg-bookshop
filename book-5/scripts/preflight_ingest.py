"""Preflight for a Firehose Iceberg destination, run against a local table.

Checks only what AWS documents as a prerequisite (sources C6-12, C6-13, C6-17,
C3-09). Local SQLite catalog; nothing here calls AWS.
"""
import os, shutil, sys
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestamptzType, DecimalType

HERE = os.path.dirname(os.path.abspath(__file__))
WH = os.path.join(HERE, "wh6")
shutil.rmtree(WH, ignore_errors=True); os.makedirs(WH)
cat = SqlCatalog("local", uri=f"sqlite:///{WH}/cat.db", warehouse=f"file://{WH}")
cat.create_namespace("bookshop")

good = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "status", StringType(), required=False),
    NestedField(3, "updated_at", TimestamptzType(), required=False),
    NestedField(4, "total", DecimalType(10, 2), required=False),
    identifier_field_ids=[1],
)
bad = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "orderStatus", StringType(), required=False),
    NestedField(3, "updated_at", TimestamptzType(), required=False),
)
cat.create_table("bookshop.order_changes", good, properties={"format-version": "2"})
cat.create_table("bookshop.order_changes_v1", bad, properties={"format-version": "2"})

def preflight(t, operations):
    problems = []
    fv = t.metadata.format_version
    if fv != 2:
        problems.append(f"format-version {fv}: Firehose writes v2 tables only")
    for f in t.schema().fields:
        if f.name != f.name.lower():
            problems.append(f"column {f.name!r} has upper case: invisible through the Glue Data Catalog")
    if operations & {"update", "delete"} and not t.schema().identifier_field_ids:
        problems.append("no identifier-field-ids: update and delete deliveries will fail")
    return problems

for name in ["order_changes", "order_changes_v1"]:
    t = cat.load_table(f"bookshop.{name}")
    p = preflight(t, {"insert", "update", "delete"})
    ids = [t.schema().find_field(i).name for i in t.schema().identifier_field_ids]
    print(f"{name:<20} v{t.metadata.format_version}  identifier={ids}  ->", "OK" if not p else "REFUSE")
    for x in p:
        print("   -", x)
