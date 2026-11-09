"""Keep failure, missing delivery and correctness in the denominator."""
from __future__ import annotations

import math
import statistics


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(p * len(values)) - 1)]


def summarize(attempts: list[dict]) -> dict:
    if not attempts:
        raise ValueError("An empty evaluation has no success rate")
    ids = [row["attempt_id"] for row in attempts]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate attempt identifiers")
    for row in attempts:
        if type(row.get('delivered')) is not bool or (row.get('correct') is not None and type(row['correct']) is not bool):
            raise ValueError('Delivery and correctness must be booleans; correctness may be unscored/null')
        for field in ('latency_ms','cost_usd'):
            value=row.get(field)
            if value is not None and (type(value) not in (int,float) or not math.isfinite(value) or value<0):
                raise ValueError('Observed latency and cost must be finite nonnegative numbers')
    latencies = [r["latency_ms"] for r in attempts if r.get("latency_ms") is not None]
    costs = [r["cost_usd"] for r in attempts if r.get("cost_usd") is not None]
    delivered = sum(bool(r.get("delivered")) for r in attempts)
    correct = sum(bool(r.get("delivered") and r.get("correct")) for r in attempts)
    categories = {}
    for row in attempts:
        category = row.get("failure_category") or ("success" if row.get("delivered") and row.get("correct") else "unscored_or_missing")
        categories[category] = categories.get(category, 0) + 1
    return {"attempts": len(attempts), "delivered": delivered, "correct_and_delivered": correct,
            "success_rate": correct / len(attempts), "delivery_rate": delivered / len(attempts),
            "median_latency_ms": statistics.median(latencies) if latencies else None,
            "p95_latency_ms": percentile(latencies, .95), "latency_observations": len(latencies),
            "reported_cost_usd": sum(costs) if costs else None, "cost_observations": len(costs),
            "failures": categories}


def release_decision(attempts: list[dict], *, expected_case_ids: set[str] | None = None, minimum_repeats: int = 3, minimum_success_rate: float = .9) -> dict:
    result = summarize(attempts)
    cases = {}
    for row in attempts:
        cases[row["case_id"]] = cases.get(row["case_id"], 0) + 1
    reasons = []
    if not expected_case_ids:
        reasons.append('case_manifest_missing')
    elif set(cases)!=set(expected_case_ids):
        reasons.append('case_coverage')
    if any(n < minimum_repeats for n in cases.values()):
        reasons.append("insufficient_repeats")
    if any(r.get("policy_violation") for r in attempts):
        reasons.append("policy_violation")
    if any(r.get("correct") is None for r in attempts):
        reasons.append("unscored_attempt")
    if result["success_rate"] < minimum_success_rate:
        reasons.append("success_threshold")
    case_summaries={case:summarize([row for row in attempts if row['case_id']==case]) for case in cases}
    if any(value['success_rate']<minimum_success_rate for value in case_summaries.values()):
        reasons.append('case_success_threshold')
    return {"accepted": not reasons, "reasons": reasons, "summary": result,'cases':case_summaries}
