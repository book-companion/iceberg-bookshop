"""The semantic model is application data, not a set of portable catalog views."""
from .contracts import MetricRequest, Refused, digest

MODEL = {
    "version": "bookshop-semantics-1",
    "grain": "one row per order_id, with current refund amount",
    "currency": "USD",
    "time": "order_date, inclusive start and exclusive end",
    "net_revenue": {
        "definition": "gross_cents minus discount_cents minus refund_cents for paid orders",
        "unit": "USD cents",
        "owner": "finance",
        "sql": "SELECT COALESCE(SUM(gross_cents - discount_cents - refund_cents), 0) AS net_revenue_cents FROM orders WHERE status = ? AND currency = ? AND order_date >= ? AND order_date < ?",
    },
    "active_customers": {
        "definition": "distinct customers with a paid order in the requested date window",
        "unit": "customers",
        "owner": "finance",
        "sql": "SELECT COUNT(DISTINCT customer_id) AS active_customers FROM orders WHERE status = ? AND currency = ? AND order_date >= ? AND order_date < ?",
    },
}


def query_for(request: MetricRequest) -> tuple[str, list[str]]:
    request.validate()
    if request.metric not in ("net_revenue", "active_customers"):
        raise Refused("unknown_metric", "Choose an approved metric after inspecting its definition")
    return MODEL[request.metric]["sql"], ["paid", request.currency, request.start, request.end]


MODEL_SHA256 = digest(MODEL)
