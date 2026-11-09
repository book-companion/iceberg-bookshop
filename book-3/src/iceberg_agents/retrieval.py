"""A deterministic embedding fixture and transactional current-state index.

The hashed vector is useful for inspecting mechanics, not a claim about learned
retrieval quality. A complete id inventory supplies deletions; a row-lineage
watermark alone cannot do that.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from pathlib import Path

from .contracts import Refused, canonical, digest, now

EMBEDDING_VERSION = "token-hash-128-v1"


def embed(text: str) -> list[float]:
    vector = [0.0] * 128
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        value = hashlib.sha256(token.encode()).digest()
        index = int.from_bytes(value[:4], "big") % len(vector)
        vector[index] += 1 if value[4] % 2 else -1
    length = math.sqrt(sum(v * v for v in vector))
    return [v / length for v in vector] if length else vector


class Index:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS documents(id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, text TEXT NOT NULL, sequence INTEGER NOT NULL, vector TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS position(singleton INTEGER PRIMARY KEY CHECK(singleton=1), table_uuid TEXT NOT NULL, snapshot_id INTEGER NOT NULL, sequence INTEGER NOT NULL, embedding_version TEXT NOT NULL, committed_at TEXT NOT NULL, input_sha256 TEXT NOT NULL);
        """)

    def sync(self, *, table_uuid: str, snapshot_id: int, sequence: int, current_rows: list[dict], fail_before_checkpoint: bool = False) -> dict:
        """Input is the complete key set at one captured snapshot.

        New/updated vectors are selected using last-updated sequence. Missing
        ids are deleted by reconciliation, in the same transaction as position.
        """
        if type(snapshot_id) is not int or type(sequence) is not int or sequence < 0:
            raise Refused("source_position", "Snapshot and sequence must be concrete source values")
        ids = [row["_row_id"] for row in current_rows]
        if len(ids) != len(set(ids)) or any(type(i) is not int for i in ids):
            raise Refused("source_identity", "The captured source contains duplicate or invalid row ids")
        if any(not 0 <= row["_last_updated_sequence_number"] <= sequence for row in current_rows):
            raise Refused("source_sequence", "A row sequence lies outside the captured source boundary")
        signature = digest(sorted(current_rows, key=lambda row: row["_row_id"]))
        previous = self.db.execute("SELECT table_uuid,snapshot_id,sequence,embedding_version,input_sha256 FROM position WHERE singleton=1").fetchone()
        if previous and (previous[0] != table_uuid or previous[3] != EMBEDDING_VERSION):
            raise Refused("rebuild_required", "Table identity or embedding version changed")
        if previous and sequence < previous[2]:
            raise Refused("source_rewind", "A rewind needs an explicitly rebuilt index")
        if previous and sequence == previous[2]:
            if snapshot_id == previous[1] and signature == previous[4]:
                return {"changed": 0, "deleted": 0, "replayed": True, "snapshot_id": snapshot_id}
            raise Refused("ambiguous_position", "The same sequence describes a different source image")
        watermark = previous[2] if previous else -1
        existing = {r[0]: r[1:] for r in self.db.execute("SELECT id,tenant,text,sequence FROM documents")}
        updated = [row for row in current_rows if row["_last_updated_sequence_number"] > watermark or row["_row_id"] not in existing]
        for row in current_rows:
            old = existing.get(row["_row_id"])
            if old and row["_last_updated_sequence_number"] <= watermark and (old[0], old[1]) != (row["tenant"], row["text"]):
                raise Refused("lineage_contract", "Content changed without an advancing row sequence")
        deleted = set(existing) - set(ids)
        with self.db:
            self.db.executemany("DELETE FROM documents WHERE id=?", [(i,) for i in deleted])
            self.db.executemany("INSERT INTO documents VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET tenant=excluded.tenant,text=excluded.text,sequence=excluded.sequence,vector=excluded.vector",
                                [(r["_row_id"], r["tenant"], r["text"], r["_last_updated_sequence_number"], canonical(embed(r["text"]))) for r in updated])
            if fail_before_checkpoint:
                raise RuntimeError("Injected interruption before checkpoint")
            self.db.execute("INSERT INTO position VALUES(1,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET table_uuid=excluded.table_uuid,snapshot_id=excluded.snapshot_id,sequence=excluded.sequence,embedding_version=excluded.embedding_version,committed_at=excluded.committed_at,input_sha256=excluded.input_sha256",
                            (table_uuid, snapshot_id, sequence, EMBEDDING_VERSION, now(), signature))
        return {"changed": len(updated), "deleted": len(deleted), "replayed": False, "snapshot_id": snapshot_id}

    def search(self, text: str, tenants: tuple[str, ...], limit: int = 3) -> list[dict]:
        if not tenants or not 1 <= limit <= 20:
            return []
        query = embed(text)
        placeholders = ",".join("?" for _ in tenants)
        rows = self.db.execute(f"SELECT id,tenant,text,vector FROM documents WHERE tenant IN ({placeholders})", tenants)
        scored = [{"id": i, "tenant": t, "text": body, "score": sum(a*b for a,b in zip(query,json.loads(v),strict=True))} for i,t,body,v in rows]
        return sorted(scored, key=lambda row: (-row["score"], row["id"]))[:limit]

    def close(self):
        self.db.close()


def search_current(work: Path, subject: str, text: str) -> dict:
    """Require the index to match the source snapshot captured for this request."""
    from .catalog import catalog, table_name
    from .contracts import principal
    who = principal(subject)
    if "orders" not in who.tables or not who.tenants:
        raise Refused("access_denied", "No retrieval access for this identity")
    path = work / "retrieval.sqlite"
    if not path.is_file():
        raise Refused("index_missing", "Build the derived index before retrieval")
    table = catalog(work).load_table(table_name())
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise Refused("missing_snapshot", "No source snapshot is available")
    index = Index(path)
    try:
        index.db.execute("BEGIN")
        position = index.db.execute("SELECT table_uuid,snapshot_id,sequence,embedding_version,committed_at FROM position WHERE singleton=1").fetchone()
        expected = (str(table.metadata.table_uuid), snapshot.snapshot_id, snapshot.sequence_number, EMBEDDING_VERSION)
        if not position or tuple(position[:4]) != expected:
            raise Refused("stale_index", "Synchronize the index to the captured source state before retrieval")
        return {"table_uuid":position[0],"snapshot_id":position[1],"sequence":position[2],
                "embedding_version":position[3],"index_committed_at":position[4],
                "documents":index.search(text,who.tenants),"content_is_untrusted":True}
    finally:
        index.db.rollback()
        index.close()
