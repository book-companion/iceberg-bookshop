"""Deterministic observations and proposals; this module executes no maintenance."""
from __future__ import annotations

from .contracts import Refused, digest


def assess(metrics: dict) -> list[dict]:
    required = {"table", "snapshot_id", "observed_at", "data_files", "small_files", "commit_age_seconds", "retention_hold"}
    if not required <= metrics.keys():
        raise Refused("missing_metrics", "A diagnostic input must identify its observation boundary and counters")
    for name in ("data_files", "small_files", "commit_age_seconds"):
        if type(metrics[name]) not in (int, float) or metrics[name] < 0:
            raise Refused("invalid_metric", "Counters must be nonnegative numbers")
    if metrics["small_files"] > metrics["data_files"]:
        raise Refused("invalid_metric", "Small files cannot exceed all data files")
    findings = []
    if metrics["data_files"] >= 20 and metrics["small_files"] / metrics["data_files"] > 0.5:
        findings.append({"code": "fragmentation_candidate", "evidence": ["data_files", "small_files"], "meaning": "Inspect a bounded rewrite candidate set; this is not proof of query benefit"})
    if metrics["commit_age_seconds"] > metrics.get("freshness_slo_seconds", 3600):
        findings.append({"code": "stale_commit", "evidence": ["commit_age_seconds"], "meaning": "Compare producer progress and expected update cadence before diagnosing the cause"})
    if metrics["retention_hold"]:
        findings.append({"code": "retention_hold", "evidence": ["retention_hold"], "meaning": "An owner must release the hold before an expiry proposal is eligible"})
    return findings


def plan(metrics: dict, *, max_rewrite_bytes: int, candidate_bytes: int, concurrent_writers: bool) -> dict:
    if type(max_rewrite_bytes) is not int or max_rewrite_bytes <= 0 or type(candidate_bytes) is not int or candidate_bytes < 0 or type(concurrent_writers) is not bool:
        raise Refused("plan_budget", "Use a positive integer byte budget, nonnegative candidate bytes and an explicit writer state")
    findings = assess(metrics)
    reasons = []
    if candidate_bytes <= 0:
        reasons.append("no_candidate_bytes")
    if candidate_bytes > max_rewrite_bytes:
        reasons.append("rewrite_budget")
    if concurrent_writers:
        reasons.append("writer_coordination_required")
    if not any(x["code"] == "fragmentation_candidate" for x in findings):
        reasons.append("fragmentation_not_established")
    return {"kind": "proposal", "table": metrics["table"], "base_snapshot_id": metrics["snapshot_id"],
            "table_uuid":metrics.get("table_uuid"),"observed_at":metrics["observed_at"],
            "observation_sha256": digest(metrics), "action": "rewrite_selected_data_files", "candidate_bytes": candidate_bytes,
            "max_rewrite_bytes":max_rewrite_bytes,"concurrent_writers":concurrent_writers,
            "candidate_selection":{"kind":"small_files","threshold_bytes":metrics.get("small_file_threshold_bytes"),"operator_must_resolve_exact_files":True},
            "retention_hold":metrics["retention_hold"],"benefit_hypothesis":"Fewer file opens may reduce planning or scan overhead; measure the actual workload",
            "eligible_for_operator_review": not reasons, "blocking_reasons": reasons, "executes": False,
            "excluded_actions": ["expire_snapshots", "remove_orphan_files"], "expected_savings": None}


def incident_packet(observations: list[dict], hypotheses: list[dict]) -> dict:
    facts = {row["id"]: row for row in observations}
    if len(facts) != len(observations) or any(not {"id", "observed_at", "signal", "value"} <= row.keys() for row in observations):
        raise Refused("incident_evidence", "Every observation needs a unique id, timestamp, signal and value")
    for item in hypotheses:
        if not item.get("evidence_ids") or any(i not in facts for i in item["evidence_ids"]):
            raise Refused("unsupported_hypothesis", "A hypothesis must cite existing observations")
        if not item.get("next_check"):
            raise Refused("missing_check", "A hypothesis needs a discriminating next check")
    return {"observations": observations, "hypotheses": hypotheses, "recovery_executed": False}
