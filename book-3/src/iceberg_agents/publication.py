"""Publish an exact reviewed snapshot with catalog-atomic reference assertions."""
from __future__ import annotations

import re
from pathlib import Path
from uuid import UUID

from .catalog import catalog
from .contracts import Refused


def fast_forward_exact(work: Path, table_name: str, branch: str, *, expected_uuid: str,
                       expected_base: int, expected_candidate: int) -> None:
    from pyiceberg.table.refs import SnapshotRefType
    from pyiceberg.table.update import AssertTableUUID, AssertRefSnapshotId, SetSnapshotRefUpdate
    if not re.fullmatch(r"review_[a-f0-9]{32}", branch):
        raise Refused("branch_name", "Only a generated review branch may publish")
    cat = catalog(work)
    table = cat.load_table(table_name)
    if str(table.metadata.table_uuid) != expected_uuid:
        raise Refused("table_replaced", "The approved table identity changed")
    current = table.current_snapshot()
    ref = table.metadata.refs.get(branch)
    if current is None or current.snapshot_id != expected_base:
        raise Refused("base_changed", "Main moved before the conditional commit")
    if ref is None or ref.snapshot_id != expected_candidate:
        raise Refused("candidate_changed", "The reviewed branch moved before the conditional commit")
    ancestor = table.snapshot_by_id(expected_candidate)
    seen = set()
    while ancestor and ancestor.snapshot_id != expected_base:
        if ancestor.snapshot_id in seen:
            raise Refused("invalid_history", "Snapshot ancestry contains a cycle")
        seen.add(ancestor.snapshot_id)
        ancestor = table.snapshot_by_id(ancestor.parent_snapshot_id) if ancestor.parent_snapshot_id is not None else None
    if ancestor is None:
        raise Refused("not_fast_forward", "The candidate must descend from the approved base")
    main = table.metadata.refs["main"]
    cat.commit_table(
        table,
        requirements=(AssertTableUUID(uuid=UUID(expected_uuid)),
                      AssertRefSnapshotId(ref="main", snapshot_id=expected_base),
                      AssertRefSnapshotId(ref=branch, snapshot_id=expected_candidate)),
        updates=(SetSnapshotRefUpdate(ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=expected_candidate,
                                      max_ref_age_ms=main.max_ref_age_ms,
                                      max_snapshot_age_ms=main.max_snapshot_age_ms,
                                      min_snapshots_to_keep=main.min_snapshots_to_keep),),
    )
