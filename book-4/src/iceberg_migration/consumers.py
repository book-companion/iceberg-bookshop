"""Compare actual query results at two captured images, including duplicates."""
from __future__ import annotations

from collections import Counter
from decimal import Decimal

from .common import Refused, canonical
from .fixture import arrow

QUERIES={
 'revenue': "SELECT tenant, sum(gross_cents-discount_cents-refund_cents)::BIGINT AS revenue_cents FROM orders WHERE status='paid' GROUP BY tenant ORDER BY tenant",
 'customers': "SELECT tenant,count(DISTINCT customer_id) AS customers FROM orders WHERE status='paid' GROUP BY tenant ORDER BY tenant",
 'status': 'SELECT status,count(*) AS orders FROM orders GROUP BY status ORDER BY status',
 'empty': "SELECT order_id FROM orders WHERE currency='EUR' ORDER BY order_id",
}


def value(value):
    if isinstance(value,Decimal):return {'decimal':str(value)}
    if hasattr(value,'isoformat'):return {'timestamp':value.isoformat()}
    return value


def result_equal(left,right,*,ordered=False):
    if left['columns']!=right['columns']:return False
    a=[canonical([value(v) for v in row]) for row in left['rows']]
    b=[canonical([value(v) for v in row]) for row in right['rows']]
    return a==b if ordered else Counter(a)==Counter(b)


def query(data,sql):
    import duckdb
    db=duckdb.connect(config={'threads':'1','enable_external_access':'false'})
    try:
        db.register('orders',arrow(data))
        cursor=db.execute(sql)
        return {'columns':[c[0] for c in cursor.description],'rows':cursor.fetchall()}
    finally:db.close()


def compare_queries(left,right):
    cases=[]
    for name,sql in QUERIES.items():
        a,b=query(left,sql),query(right,sql)
        cases.append({'case':name,'ordered':True,'equal':result_equal(a,b,ordered=True),'source':a,'target':b,'sql':sql})
    return {'status':'passed' if all(c['equal'] for c in cases) else 'failed','cases':cases,
            'scope':'same DuckDB engine over captured source and target images; not cross-engine certification'}
