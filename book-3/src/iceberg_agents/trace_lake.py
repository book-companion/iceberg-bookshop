"""Single-writer ingestion of a redacted operational ledger into Iceberg."""
from __future__ import annotations

import json
from pathlib import Path

from .catalog import catalog
from .contracts import Refused, canonical


def ingest(work: Path, source: Path, namespace: str = "agents_traces") -> dict:
    import re
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, LongType, DoubleType
    if not re.fullmatch(r"agents_[a-z0-9_]+", namespace):
        raise Refused("namespace", "Trace tables belong to an agents_* namespace")
    records = [json.loads(line) for line in source.read_text().splitlines() if line.strip()]
    fields = [("run_id",StringType(),pa.string()),("subject",StringType(),pa.string()),("variant",StringType(),pa.string()),
              ("state",StringType(),pa.string()),("delivered",LongType(),pa.int64()),("revision",LongType(),pa.int64()),("started_at",StringType(),pa.string()),
              ("finished_at",StringType(),pa.string()),("error_code",StringType(),pa.string()),
              ("latency_ms",DoubleType(),pa.float64()),("total_cost_usd",DoubleType(),pa.float64())]
    rows = [{name: r.get(name) for name,_,_ in fields} for r in records]
    if not rows:
        raise Refused("empty_trace_batch", "No operational attempts were supplied")
    if len({r['run_id'] for r in rows}) != len(rows):
        raise Refused("duplicate_run", "Export a single current record for each run id")
    if any(not isinstance(r['run_id'],str) or not r['run_id'] or type(r['revision']) is not int or r['revision'] < 0 for r in rows):
        raise Refused("run_identity", "Runs need a nonempty identity and a nonnegative operational revision")
    cat = catalog(work)
    cat.create_namespace_if_not_exists(namespace)
    name = namespace + ".runs"
    if not cat.table_exists(name):
        schema = Schema(*(NestedField(i,n,t,required=False) for i,(n,t,_) in enumerate(fields,1)))
        cat.create_table(name,schema=schema,properties={"format-version":"2"})
    table = cat.load_table(name)
    arrow_schema = pa.schema([pa.field(n,a) for n,_,a in fields])
    batch = pa.Table.from_pylist(rows,schema=arrow_schema)
    rows = batch.to_pylist()
    # One ingestion owner. The revision check rejects stale exports but does
    # not coordinate independent writers racing between this scan and commit.
    previous = {r['run_id']: r for r in table.scan().to_arrow().to_pylist()}
    for row in rows:
        old = previous.get(row['run_id'])
        if old and (row['revision'] < old['revision'] or (row['revision'] == old['revision'] and canonical(row) != canonical(old))):
            raise Refused("stale_run", "An older or conflicting operational revision cannot replace analytical state")
    result = table.upsert(batch,join_cols=["run_id"])
    return {"table":name,"submitted":len(rows),"snapshot_id":table.current_snapshot().snapshot_id,
            "rows_inserted":result.rows_inserted,"rows_updated":result.rows_updated}
