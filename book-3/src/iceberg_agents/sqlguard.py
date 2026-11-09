"""A deliberately small SQL surface. This is one layer, not an OS sandbox."""
from __future__ import annotations

from .contracts import Refused
from .fixture import COLUMNS


def bounded_select(sql: str, max_rows: int = 50) -> str:
    import sqlglot
    from sqlglot import exp
    if not isinstance(sql, str) or len(sql) > 8192 or not 1 <= max_rows <= 1000:
        raise Refused("query_size", "Query or result limit is outside the configured bound")
    try:
        trees = sqlglot.parse(sql, read="duckdb")
    except sqlglot.errors.ParseError as exc:
        raise Refused("invalid_sql", "The query cannot be parsed") from exc
    if len(trees) != 1 or not isinstance(trees[0], exp.Select):
        raise Refused("read_only", "Exactly one SELECT is allowed")
    tree = trees[0]
    if tree.args.get("with") or tree.args.get("with_") or tree.args.get("joins") or tree.args.get("into") or tree.args.get("offset"):
        raise Refused("unsupported_shape", "CTEs, joins, INTO and OFFSET are outside this tool's contract")
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1 or tables[0].name != "orders" or tables[0].db or tables[0].catalog:
        raise Refused("table_scope", "Only the policy-filtered orders relation may be queried")
    if any(not isinstance(t.this, exp.Identifier) for t in tables):
        raise Refused("table_scope", "Table-producing expressions are forbidden")
    for column in tree.find_all(exp.Column):
        if column.name not in COLUMNS:
            raise Refused("column_scope", "A column is not part of the exposed schema")
    allowed_nodes = {
        "Select", "From", "Table", "Identifier", "Column", "Alias", "Where", "And", "Or", "Not",
        "EQ", "NEQ", "GT", "GTE", "LT", "LTE", "Is", "Null", "Boolean", "Literal", "Placeholder",
        "Add", "Sub", "Mul", "Div", "Neg", "Paren", "Sum", "Count", "Avg", "Min", "Max",
        "Coalesce", "Distinct", "Group", "Order", "Ordered", "Limit", "Between", "In", "Star",
    }
    for node in tree.walk():
        if type(node).__name__ not in allowed_nodes:
            raise Refused("unsupported_expression", f"Expression {type(node).__name__} is outside the approved SQL subset")
        if isinstance(node, exp.Star) and not isinstance(node.parent, exp.Count):
            raise Refused("explicit_columns", "Name the result columns; only COUNT(*) may use a star")
    limit = tree.args.get("limit")
    if limit:
        value = limit.expression
        if not isinstance(value, exp.Literal) or not value.is_int or not 1 <= int(value.this) <= max_rows:
            raise Refused("result_budget", "The result limit exceeds this principal's policy")
    else:
        tree = tree.limit(max_rows)
    return tree.sql(dialect="duckdb")
