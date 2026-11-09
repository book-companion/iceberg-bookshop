"""A bounded metadata observation at one current snapshot."""
from __future__ import annotations

import time
from pathlib import Path

from .catalog import catalog, table_name
from .contracts import Refused, now


def observe(work: Path, threshold_bytes: int = 128 * 1024 * 1024) -> dict:
    table = catalog(work).load_table(table_name())
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise Refused("empty_history", "No committed snapshot to inspect")
    files = [task.file for task in table.scan(snapshot_id=snapshot.snapshot_id).plan_files()]
    small = [f for f in files if f.file_size_in_bytes < threshold_bytes]
    return {"table": table_name(), "table_uuid":str(table.metadata.table_uuid), "snapshot_id": snapshot.snapshot_id, "observed_at": now(),
            "data_files": len(files), "small_files": len(small), "small_file_threshold_bytes": threshold_bytes,
            "candidate_bytes": sum(f.file_size_in_bytes for f in small),
            "commit_age_seconds": max(0, time.time() - snapshot.timestamp_ms / 1000),
            "retention_hold": any(name != "main" for name in table.metadata.refs),
            "hold_interpretation": "conservative planning flag for any non-main reference"}
