"""Policy and evidence belong outside the model's reasoning loop."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import asdict
from pathlib import Path

from .contracts import MetricRequest, Receipt, Refused, principal, now, write_json
from .semantic import MODEL, MODEL_SHA256, query_for
from .sqlguard import bounded_select


class Gateway:
    def __init__(self, work: Path, subject: str):
        self.work = work.resolve()
        self.principal = principal(subject)

    def discover(self) -> dict:
        return {"tables": list(self.principal.tables), "metrics": ["net_revenue", "active_customers"] if self.principal.tables else []}

    def describe(self) -> dict:
        if "orders" not in self.principal.tables:
            raise Refused("access_denied", "No schema access for this principal")
        return {"semantic_model": MODEL, "semantic_sha256": MODEL_SHA256, "scope": list(self.principal.tenants)}

    def metric(self, request: MetricRequest, snapshot_id: int | None = None) -> Receipt:
        sql, parameters = query_for(request)
        return self.query(sql, parameters, snapshot_id, _metric_request=request)

    def query(self, sql: str, parameters: list | None = None, snapshot_id: int | None = None, *, _metric_request: MetricRequest | None = None) -> Receipt:
        request_id = uuid.uuid4().hex
        try:
            if "orders" not in self.principal.tables or not self.principal.tenants:
                raise Refused("access_denied", "No table access for this principal")
            if snapshot_id is not None and (type(snapshot_id) is not int or snapshot_id < 0):
                raise Refused("snapshot_id", "A snapshot id must be a nonnegative integer")
            parameters = parameters or []
            if _metric_request is not None and (sql, parameters) != query_for(_metric_request):
                raise Refused("metric_contract", "Metric evidence must execute the approved definition and arguments")
            if len(parameters) > 16 or any(type(v) not in (str, int, float, bool, type(None)) for v in parameters):
                raise Refused("parameters", "Only a short scalar parameter list is accepted")
            prepared = bounded_select(sql, self.principal.max_rows)
        except Refused as exc:
            self._trace(request_id, exc.code)
            raise
        payload = {"subject": self.principal.subject, "work": str(self.work), "sql": prepared,
                   "parameters": parameters, "snapshot_id": snapshot_id}
        try:
            completed = subprocess.run([sys.executable, "-m", "iceberg_agents.worker"],
                                       input=json.dumps(payload), text=True, capture_output=True,
                                       timeout=self.principal.timeout_seconds, check=False)
        except subprocess.TimeoutExpired as exc:
            self._trace(request_id, "query_timeout")
            raise Refused("query_timeout", "The query process exceeded its wall-clock budget") from exc
        try:
            answer = json.loads(completed.stdout)
        except (ValueError, TypeError) as exc:
            self._trace(request_id, "dependency_failed")
            raise Refused("dependency_failed", "The query process did not return a valid envelope") from exc
        if completed.returncode or not answer.get("ok"):
            code = answer.get("code", "dependency_failed")
            self._trace(request_id, code)
            raise Refused(code, answer.get("detail", "The query was refused"))
        receipt = Receipt(request_id=request_id, subject=self.principal.subject,
                          policy_sha256=self.principal.fingerprint, semantic_sha256=MODEL_SHA256,
                          metric_request=asdict(_metric_request) if _metric_request else None,
                          sql=prepared, parameters=parameters, run_id=os.environ.get("BOOK3_RUN_ID", ""), **answer["value"])
        write_json(self.work / "receipts" / f"{request_id}.json", asdict(receipt))
        self._trace(request_id, "completed", receipt.snapshot_id)
        return receipt

    def _trace(self, request_id: str, state: str, snapshot_id: int | None = None) -> None:
        self.work.mkdir(parents=True, exist_ok=True)
        with (self.work / "tool-trace.jsonl").open("a") as stream:
            stream.write(json.dumps({"request_id": request_id, "subject": self.principal.subject,
                                     "run_id": os.environ.get("BOOK3_RUN_ID", ""),
                                     "state": state, "snapshot_id": snapshot_id, "created_at": now()}) + "\n")
