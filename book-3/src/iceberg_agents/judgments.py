"""Single-writer, append-only judgments linked to already ingested runs."""
from __future__ import annotations

import json
import re
from pathlib import Path

from .catalog import catalog
from .contracts import Refused, canonical

FIELDS = {
    "tool_calls": ("call_id", "run_id", "tool", "status", "receipt_id", "observed_at"),
    "evaluations": ("evaluation_id", "run_id", "case_id", "evaluator_version", "verdict", "reason_code", "observed_at"),
    "feedback": ("feedback_id", "run_id", "reporter", "disposition", "reason_code", "observed_at"),
}


def ingest_judgments(work: Path, source: Path, kind: str, namespace: str = "agents_traces") -> dict:
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType
    if kind not in FIELDS or not re.fullmatch(r"agents_[a-z0-9_]+", namespace):
        raise Refused("judgment_target", "Choose an approved judgment table in an agents_* namespace")
    fields = FIELDS[kind]
    key = fields[0]
    records = [json.loads(line) for line in source.read_text().splitlines() if line.strip()]
    if not records or any(set(row) != set(fields) for row in records):
        raise Refused("judgment_shape", "Supply a nonempty batch with exactly the approved fields")
    if any(not isinstance(value, str) or (not value and field != "receipt_id") or len(value) > 256 for row in records for field, value in row.items()):
        raise Refused("judgment_value", "Judgment fields must be short nonempty strings; raw answer text is excluded")
    if len({row[key] for row in records}) != len(records):
        raise Refused("duplicate_judgment", "A batch must contain one record per judgment id")
    if kind != "tool_calls":
        category = "verdict" if kind == "evaluations" else "disposition"
        allowed = {"pass", "fail", "unscored"} if kind == "evaluations" else {"useful", "incorrect", "unclear", "access_concern"}
        if any(row[category] not in allowed for row in records):
            raise Refused("judgment_category", "Unknown judgment category")
    cat = catalog(work)
    run_name = namespace + ".runs"
    if not cat.table_exists(run_name):
        raise Refused("missing_runs", "Ingest the operational run projection first")
    runs = {row["run_id"] for row in cat.load_table(run_name).scan(selected_fields=("run_id",)).to_arrow().to_pylist()}
    if any(row["run_id"] not in runs for row in records):
        raise Refused("unknown_run", "A judgment references a run absent from the analytical table")
    name = namespace + "." + kind
    if not cat.table_exists(name):
        schema = Schema(*(NestedField(i, field, StringType(), required=True) for i, field in enumerate(fields, 1)))
        cat.create_table(name, schema=schema, properties={"format-version": "2"})
    table = cat.load_table(name)
    previous = {row[key]: row for row in table.scan().to_arrow().to_pylist()}
    if any(row[key] in previous and canonical(row) != canonical(previous[row[key]]) for row in records):
        raise Refused("judgment_changed", "Use a new id and evaluator version for a revised judgment")
    new = [row for row in records if row[key] not in previous]
    if new:
        schema = pa.schema([pa.field(field, pa.string(), nullable=False) for field in fields])
        table.append(pa.Table.from_pylist(new, schema=schema))
    snapshot = table.current_snapshot()
    return {"table": name, "inserted": len(new), "replayed": len(records)-len(new),
            "snapshot_id": snapshot.snapshot_id if snapshot else None}
