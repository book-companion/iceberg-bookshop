"""Replay query evidence under today's access policy, not yesterday's privilege."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from .catalog import catalog, table_name
from .contracts import Refused, principal
from .gateway import Gateway
from .semantic import MODEL_SHA256


def retain(work: Path, receipt: dict, max_age_ms: int = 30 * 24 * 60 * 60 * 1000) -> str:
    import re
    import time
    if not re.fullmatch(r"[a-f0-9]{32}", receipt["request_id"]):
        raise Refused("receipt_id", "Invalid request identifier")
    if receipt["table"] != table_name() or type(max_age_ms) is not int or max_age_ms <= 0:
        raise Refused("retention_request", "Retention needs the configured table and a positive age limit")
    table = catalog(work).load_table(table_name())
    if str(table.metadata.table_uuid) != receipt["table_uuid"]:
        raise Refused("table_replaced", "A table name is not a durable table identity")
    snapshot = table.snapshot_by_id(receipt["snapshot_id"])
    if snapshot is None:
        raise Refused("missing_snapshot", "The evidence snapshot is no longer retained")
    # Iceberg expires non-main refs by referenced-snapshot age, not tag creation.
    if time.time() * 1000 - snapshot.timestamp_ms >= max_age_ms:
        raise Refused("retention_age", "The snapshot is already outside this reference age policy")
    tag = "evidence_" + receipt["request_id"]
    existing = table.metadata.refs.get(tag)
    if existing:
        if existing.snapshot_id != receipt["snapshot_id"]:
            raise Refused("tag_conflict", "An evidence tag already names another snapshot")
        return tag
    with table.manage_snapshots() as changes:
        changes.create_tag(receipt["snapshot_id"], tag, max_ref_age_ms=max_age_ms)
    return tag


def replay(work: Path, subject: str, receipt: dict) -> dict:
    who = principal(subject)
    if who.subject != receipt["subject"] or who.fingerprint != receipt["policy_sha256"]:
        raise Refused("policy_changed", "Re-authorize this evidence under the current subject and policy")
    if MODEL_SHA256 != receipt["semantic_sha256"]:
        raise Refused("meaning_changed", "Replay requires the archived semantic-model version")
    if receipt["table"] != table_name():
        raise Refused("table_changed", "The configured table differs from the receipt")
    table = catalog(work).load_table(table_name())
    if str(table.metadata.table_uuid) != receipt["table_uuid"]:
        raise Refused("table_replaced", "The original table identity is no longer configured")
    fresh = Gateway(work, subject).query(receipt["sql"], receipt["parameters"], receipt["snapshot_id"])
    return {"matches": fresh.result_sha256 == receipt["result_sha256"], "original_request_id": receipt["request_id"], "replay": asdict(fresh)}
