"""Can PyIceberg create a v3 table? Local SQLite catalog; nothing calls AWS."""
import os, shutil
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType

WH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wh5")
shutil.rmtree(WH, ignore_errors=True); os.makedirs(WH)
cat = SqlCatalog("local", uri=f"sqlite:///{WH}/cat.db", warehouse=f"file://{WH}")
cat.create_namespace("bookshop")
schema = Schema(NestedField(1, "order_id", LongType(), required=True))
for version in ("2", "3"):
    try:
        t = cat.create_table(f"bookshop.orders_v{version}", schema,
                             properties={"format-version": version})
        print(f"format-version {version}: created, v{t.metadata.format_version}")
    except Exception as e:
        print(f"format-version {version}: {type(e).__name__}: {e}")
