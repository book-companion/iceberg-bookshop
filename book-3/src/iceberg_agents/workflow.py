"""A reviewable publication protocol, with a separate operator entry point.

The SQLite ledger assumes trusted local operators. Deployment must authenticate
reviewers independently and protect both ledger and catalog write credentials.
The model server does not import this module or receive these operations.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from .contracts import Refused, canonical, digest, now


def validate_change(before: list[dict], after: list[dict], correction: dict) -> dict:
    for rows in (before, after):
        keys = [r["order_id"] for r in rows]
        if len(keys) != len(set(keys)):
            raise Refused("duplicate_key", "Publication requires a unique order_id")
    original = {r["order_id"]: r for r in before}
    candidate = {r["order_id"]: r for r in after}
    if original.keys() != candidate.keys():
        raise Refused("row_set_changed", "A refund correction must not insert or remove orders")
    key, refund = correction["order_id"], correction["refund_cents"]
    if type(key) is not int or type(refund) is not int or key not in original:
        raise Refused("invalid_correction", "The correction needs an existing integer order id and integer cents")
    row = original[key]
    if row["status"] != "paid" or not 0 <= refund <= row["gross_cents"] - row["discount_cents"]:
        raise Refused("invalid_refund", "Refund must fit the paid order's net price before refunds")
    expected = {k: dict(v) for k, v in original.items()}
    expected[key]["refund_cents"] = refund
    if expected != candidate:
        raise Refused("unexpected_diff", "The candidate changes more than the approved refund field")
    return {"valid": True, "row_count": len(candidate), "changed_order_id": key,
            "before_sha256": digest(sorted(before, key=lambda r:r["order_id"])),
            "after_sha256": digest(sorted(after, key=lambda r:r["order_id"])), "validator_version": "refund-validator-1"}


class Workflow:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE IF NOT EXISTS changes(
          change_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, table_uuid TEXT NOT NULL,
          base_snapshot INTEGER NOT NULL, candidate_snapshot INTEGER, branch TEXT NOT NULL,
          proposer TEXT NOT NULL, correction TEXT NOT NULL, validation TEXT,
          reviewer TEXT, approved_digest TEXT, state TEXT NOT NULL, updated_at TEXT NOT NULL)""")

    def get(self, change_id: str) -> dict:
        row = self.db.execute("SELECT * FROM changes WHERE change_id=?", (change_id,)).fetchone()
        if row is None:
            raise Refused("unknown_change", "No such change proposal")
        value = dict(row)
        for key in ("correction", "validation"):
            if value[key] is not None:
                value[key] = json.loads(value[key])
        return value

    def propose(self, *, table: str, table_uuid: str, base_snapshot: int, proposer: str, order_id: int, refund_cents: int) -> dict:
        if not proposer or type(order_id) is not int or type(refund_cents) is not int or refund_cents < 0:
            raise Refused("invalid_proposal", "Identify the proposer, order and nonnegative integer refund")
        change_id = uuid.uuid4().hex
        self.db.execute("INSERT INTO changes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (change_id, table, table_uuid, base_snapshot, None, "review_"+change_id, proposer,
                         canonical({"order_id": order_id, "refund_cents": refund_cents}), None, None, None, "proposed", now()))
        return self.get(change_id)

    def staged(self, change_id: str, candidate_snapshot: int):
        value = self.get(change_id)
        if value["state"] not in ("proposed", "staged"):
            raise Refused("wrong_state", "Staging cannot change reviewed evidence")
        self.db.execute("UPDATE changes SET candidate_snapshot=?,state='staged',updated_at=? WHERE change_id=?", (candidate_snapshot, now(), change_id))

    def validated(self, change_id: str, report: dict):
        value = self.get(change_id)
        if value["state"] != "staged" or not report.get("valid"):
            raise Refused("wrong_state", "Only a staged candidate with a passing validator can advance")
        self.db.execute("UPDATE changes SET validation=?,state='validated',updated_at=? WHERE change_id=?", (canonical(report), now(), change_id))

    def stage_with_bridge(self, change_id: str, bridge):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            value=self.get(change_id)
            if value['state'] not in ('proposed','staged'):
                raise Refused('wrong_state','Check review state before any branch mutation')
            self.staged(change_id,bridge.stage(value))
            self.db.commit()
        except BaseException:
            self.db.rollback();raise

    def validate_with_bridge(self, change_id: str, bridge):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            value=self.get(change_id)
            if value['state']!='staged':
                raise Refused('wrong_state','Only the current staged candidate may be validated')
            if bridge.branch_snapshot(value['branch'])!=value['candidate_snapshot']:
                raise Refused('candidate_changed','Branch changed before validation')
            report=validate_change(bridge.rows_at(value['base_snapshot']),bridge.rows_at(value['candidate_snapshot']),value['correction'])
            self.validated(change_id,report)
            self.db.commit()
        except BaseException:
            self.db.rollback();raise

    @staticmethod
    def approval_digest(value: dict) -> str:
        return digest({k: value[k] for k in ("table_name", "table_uuid", "base_snapshot", "candidate_snapshot", "branch", "correction", "validation")})

    def approve(self, change_id: str, reviewer: str):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            value = self.get(change_id)
            if value["state"] != "validated" or not reviewer or reviewer == value["proposer"]:
                raise Refused("approval_refused", "A different identified reviewer must approve validated evidence")
            self.db.execute("UPDATE changes SET reviewer=?,approved_digest=?,state='approved',updated_at=? WHERE change_id=?", (reviewer, self.approval_digest(value), now(), change_id))
            self.db.commit()
        except BaseException:
            self.db.rollback();raise

    def publish(self, change_id: str, bridge) -> dict:
        # Serialize this local publisher. Catalog-level concurrency still needs
        # atomic reference assertions; this lock does not fence other writers.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            value = self.get(change_id)
            if value["state"] == "published":
                self.db.commit()
                return value
            if value["state"] != "approved" or value["approved_digest"] != self.approval_digest(value):
                raise Refused("approval_refused", "The exact candidate evidence must have an intact approval")
            current = bridge.identity()
            if current["table_uuid"] != value["table_uuid"]:
                raise Refused("table_replaced", "The approved table identity changed")
            candidate = bridge.branch_snapshot(value["branch"])
            if candidate != value["candidate_snapshot"]:
                raise Refused("candidate_changed", "The branch moved after validation")
            if current["snapshot_id"] == candidate:
                # Retry after a successful catalog commit and a lost ledger write.
                pass
            elif current["snapshot_id"] != value["base_snapshot"]:
                raise Refused("base_changed", "Main moved; restage and revalidate against its new base")
            else:
                bridge.fast_forward(value["branch"], expected_uuid=value["table_uuid"],
                                    expected_base=value["base_snapshot"], expected_candidate=candidate)
                observed = bridge.identity()
                if observed["snapshot_id"] != candidate:
                    raise Refused("publication_unknown", "Inspect the catalog before retrying publication")
            self.db.execute("UPDATE changes SET state='published',updated_at=? WHERE change_id=?", (now(), change_id))
            self.db.commit()
            return self.get(change_id)
        except BaseException:
            self.db.rollback()
            raise

    def close(self):
        self.db.close()
