"""A disposable query process with a parent-enforced wall-clock limit."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from .catalog import catalog, table_name
from .contracts import principal, Refused, result_digest


def execute(payload: dict) -> dict:
    import duckdb
    from pyiceberg.expressions import In
    who = principal(payload["subject"])
    if "orders" not in who.tables or not who.tenants:
        raise Refused("access_denied", "No table access for this principal")
    try:
        table = catalog(Path(payload["work"])).load_table(table_name())
    except Refused:
        raise
    except Exception as exc:
        raise Refused("catalog_failed", "The configured catalog did not supply the table") from exc
    snap = table.snapshot_by_id(payload["snapshot_id"]) if payload.get("snapshot_id") is not None else table.current_snapshot()
    if snap is None:
        raise Refused("missing_snapshot", "The requested evidence is not retained")
    schema_id = snap.schema_id if snap.schema_id is not None else table.metadata.current_schema_id
    scan = table.scan(snapshot_id=snap.snapshot_id, row_filter=In("tenant", who.tenants))
    # A planning bound, distinct from the query's returned-row LIMIT. This budget
    # is intentionally conservative: a selected file can contain other tenants.
    planned_rows = sum(task.file.record_count for task in scan.plan_files())
    if planned_rows > who.source_row_budget:
        raise Refused("source_budget", "Selected files exceed the teaching gateway's source-row budget")
    try:
        arrow = scan.to_arrow()
    except Exception as exc:
        raise Refused("source_read_failed", "The selected source data could not be read") from exc
    if arrow.num_rows > who.source_row_budget:
        raise Refused("source_budget", "Materialized rows exceed the source-row budget")
    keys = arrow.column("order_id").to_pylist()
    if any(key is None for key in keys) or len(keys) != len(set(keys)):
        raise Refused("invalid_grain", "The authorized relation must contain one row per order id")
    con = None
    try:
        con = duckdb.connect(config={"threads": 1, "memory_limit": "256MB", "enable_external_access": False})
        con.register("orders", arrow)
        cursor = con.execute(payload["sql"], payload["parameters"])
        names = [col[0] for col in cursor.description]
        if len(names) != len(set(names)):
            raise Refused("duplicate_columns", "Result aliases must be unique")
        values = cursor.fetchmany(who.max_rows + 1)
        if len(values) > who.max_rows:
            raise Refused("result_budget", "Result exceeds the configured row budget")
        rows = [dict(zip(names, row, strict=True)) for row in values]
        return {"table": table_name(), "table_uuid": str(table.metadata.table_uuid), "snapshot_id": snap.snapshot_id,
                "schema_id": schema_id, "metadata_location": table.metadata_location, "rows": rows,
                "result_sha256": result_digest(rows)}
    except Refused:
        raise
    except Exception as exc:
        raise Refused("engine_failed", "The query engine did not complete the accepted request") from exc
    finally:
        if con is not None: con.close()


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        value = execute(payload)
        print(json.dumps({"ok": True, "value": value}, default=str))
        return 0
    except Refused as exc:
        print(json.dumps({"ok": False, "code": exc.code, "detail": str(exc)}))
        return 2
    except Exception:
        # The trusted operator can rerun a stack probe for a traceback. A tool
        # result must not expose URLs containing credentials or driver internals.
        print(json.dumps({"ok": False, "code": "dependency_failed", "detail": "The catalog or query dependency failed"}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
