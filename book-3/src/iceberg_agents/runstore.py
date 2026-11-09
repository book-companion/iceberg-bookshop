"""An operational ledger; exported analytical traces have a separate lifecycle."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .contracts import canonical, now


class RunStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY, subject TEXT NOT NULL, variant TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('started','completed','failed')),
          delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0,1)),
          revision INTEGER NOT NULL DEFAULT 0,
          started_at TEXT NOT NULL, finished_at TEXT, result_json TEXT, error_code TEXT,
          latency_ms REAL, total_cost_usd REAL
        );
        CREATE TABLE IF NOT EXISTS tool_calls (
          run_id TEXT NOT NULL REFERENCES runs(run_id), call_id TEXT NOT NULL,
          tool TEXT NOT NULL, status TEXT NOT NULL, receipt_id TEXT,
          observed_at TEXT NOT NULL,
          PRIMARY KEY(run_id,call_id)
        );
        """)

    def start(self, run_id: str, subject: str, variant: str):
        with self.db:
            self.db.execute("INSERT INTO runs(run_id,subject,variant,state,started_at) VALUES(?,?,?,'started',?)", (run_id, subject, variant, now()))

    def finish(self, run_id: str, result: dict | None, *, error_code: str | None = None, latency_ms: float | None = None, cost: float | None = None):
        with self.db:
            cur = self.db.execute("UPDATE runs SET state=?,finished_at=?,result_json=?,error_code=?,latency_ms=?,total_cost_usd=?,revision=revision+1 WHERE run_id=? AND state='started'",
                                  ("failed" if error_code else "completed", now(), canonical(result) if result is not None else None, error_code, latency_ms, cost, run_id))
            if cur.rowcount != 1:
                raise ValueError("A run may finish exactly once after it starts")

    def acknowledge(self, run_id: str):
        """Called by the recipient after receiving the complete envelope."""
        with self.db:
            cur = self.db.execute("UPDATE runs SET revision=revision+CASE WHEN delivered=0 THEN 1 ELSE 0 END,delivered=1 WHERE run_id=? AND state IN ('completed','failed')", (run_id,))
            if cur.rowcount != 1:
                raise ValueError("Cannot acknowledge an absent or unfinished run")

    def record_tool(self, run_id: str, call_id: str, tool: str, status: str, receipt_id: str | None = None):
        with self.db:
            self.db.execute("INSERT INTO tool_calls VALUES(?,?,?,?,?,?)", (run_id, call_id, tool, status, receipt_id or "", now()))

    def export_tools(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db.row_factory = sqlite3.Row
        rows = self.db.execute("SELECT * FROM tool_calls ORDER BY observed_at,call_id")
        path.write_text("".join(canonical(dict(row)) + "\n" for row in rows))

    def rows(self) -> list[dict]:
        self.db.row_factory = sqlite3.Row
        return [dict(row) for row in self.db.execute("SELECT * FROM runs ORDER BY started_at,run_id")]

    def export(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # An allowlist prevents future operational fields from silently becoming
        # analytical exports. Result text can contain sensitive tool data.
        fields = ("run_id", "subject", "variant", "state", "delivered", "revision", "started_at",
                  "finished_at", "error_code", "latency_ms", "total_cost_usd")
        path.write_text("".join(canonical({key: row[key] for key in fields}) + "\n"
                                for row in self.rows()))

    def close(self):
        self.db.close()
