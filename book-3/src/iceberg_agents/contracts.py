"""Small, inspectable contracts shared by the gateway, CLI and probes."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


class Refused(ValueError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(detail)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(canonical(value) + "\n")
    tmp.replace(path)


@dataclass(frozen=True)
class Principal:
    subject: str
    tenants: tuple[str, ...]
    tables: tuple[str, ...] = ("orders",)
    max_rows: int = 50
    timeout_seconds: int = 10
    source_row_budget: int = 10000
    policy_version: str = "bookshop-policy-1"

    @property
    def fingerprint(self) -> str:
        return digest(self.__dict__)


PRINCIPALS = {
    "finance": Principal("finance", ("north", "south")),
    "north_analyst": Principal("north_analyst", ("north",)),
    "outsider": Principal("outsider", (), ()),
}


def principal(name: str) -> Principal:
    try:
        return PRINCIPALS[name]
    except KeyError as exc:
        raise Refused("unknown_identity", "No configured policy for this subject") from exc


@dataclass(frozen=True)
class MetricRequest:
    metric: str
    start: str = "2026-09-01"
    end: str = "2026-10-01"
    currency: str = "USD"

    def validate(self) -> None:
        try:
            lower, upper = date.fromisoformat(self.start), date.fromisoformat(self.end)
        except ValueError as exc:
            raise Refused("invalid_date", "Dates must be ISO calendar dates") from exc
        if lower >= upper:
            raise Refused("invalid_window", "The exclusive end must follow the inclusive start")
        if self.currency != "USD":
            raise Refused("unsupported_currency", "The fixture defines USD only; no implicit FX conversion")


@dataclass
class Receipt:
    request_id: str
    subject: str
    policy_sha256: str
    semantic_sha256: str
    table: str
    table_uuid: str
    snapshot_id: int
    schema_id: int
    metadata_location: str
    sql: str
    parameters: list[Any]
    rows: list[dict[str, Any]]
    result_sha256: str
    run_id: str = ""
    metric_request: dict[str, str] | None = None
    created_at: str = field(default_factory=now)


def result_digest(rows: list[dict[str, Any]]) -> str:
    """Compare a multiset: ordering is not a promise unless the query defines it."""
    return digest(sorted((canonical(row) for row in rows)))
