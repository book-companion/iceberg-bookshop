"""Chapter 12: the convergence test.

Compares PostgreSQL's `orders` with the Iceberg landing table on four things at
once — row count, per-status counts, and two key sums — and polls until they agree.
A count alone passes when an update was lost; a status distribution alone passes
when two rows swapped states; the key sums catch a row replaced by another row.

  python cdc/convergence_test.py [ice.<ns>.<table>] [--timeout 300]

Needs the CDC pipeline running (`flink/sql/stage4_cdc.sql`, Debezium registered as
the chapter describes) and `make -C book-2 up`. Exits non-zero if not converged.
"""
import sys, time
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from lib.platform import spark, psql


def pg_state():
    n, s1, s2 = psql("SELECT count(*), coalesce(sum(order_id),0), coalesce(sum(customer_id),0) FROM orders").split("|")
    by = dict((s, int(c)) for s, c in (l.split("|") for l in psql("SELECT status, count(*) FROM orders GROUP BY 1 ORDER BY 1").splitlines()))
    return int(n), int(s1), int(s2), by


def ice_state(sql, t):
    r = sql(f"SELECT count(*), coalesce(sum(order_id),0), coalesce(sum(customer_id),0) FROM {t}")[0]
    by = {x[0]: x[1] for x in sql(f"SELECT status, count(*) FROM {t} GROUP BY 1 ORDER BY 1")}
    return int(r[0]), int(r[1]), int(r[2]), by


def converge(sql, t, timeout=300):
    want = pg_state(); t0 = time.time()
    while time.time() - t0 < timeout:
        got = ice_state(sql, t)
        if got == want:
            return time.time() - t0, want
        time.sleep(3)
    raise AssertionError(f"not converged after {timeout}s: postgres {want} / iceberg {got}")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    table = args[0] if args else "ice.bookshop.orders_cdc"
    timeout = int(sys.argv[sys.argv.index("--timeout") + 1]) if "--timeout" in sys.argv else 300
    s = spark("convergence-test", cores=2); sql = lambda q: s.sql(q).collect()
    dt, state = converge(sql, table, timeout)
    print(f"CONVERGED in {dt:.0f}s — rows {state[0]:,}, statuses {state[3]}, sum(order_id) {state[1]:,}")
    s.stop()
