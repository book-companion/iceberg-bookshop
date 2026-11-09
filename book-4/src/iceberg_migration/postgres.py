"""PostgreSQL 18 offload at one repeatable-read snapshot, not a CDC boundary."""
from __future__ import annotations

import os
import re
import uuid
from datetime import datetime

from .common import Refused, write_json
from .consumers import QUERIES, query, result_equal
from .fixture import FIELDS, rows, packet, create_table, identity, json_rows,target_packet
from .reconcile import compare


def connect():
    import psycopg
    if not os.environ.get('MIGR_POSTGRES_DSN'):raise Refused('Set MIGR_POSTGRES_DSN privately for the owned sqlverify source')
    db=psycopg.connect(os.environ['MIGR_POSTGRES_DSN'])
    version,name=db.execute('SELECT current_setting(\'server_version_num\')::int,current_database()').fetchone()
    if version//10000!=18 or name!='sqlverify':
        db.close();raise Refused('This source profile requires PostgreSQL 18 and the sqlverify database')
    db.commit()
    return db


def offload(work):
    from psycopg import sql
    db=connect();schema='migr_'+uuid.uuid4().hex[:12]
    try:
        with db.transaction():
            db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            db.execute(sql.SQL('CREATE TABLE {}.orders (order_id BIGINT PRIMARY KEY,tenant TEXT NOT NULL,customer_id BIGINT NOT NULL,ordered_at TIMESTAMPTZ NOT NULL,status TEXT NOT NULL,currency TEXT NOT NULL,gross_cents BIGINT NOT NULL,discount_cents BIGINT NOT NULL,refund_cents BIGINT NOT NULL)').format(sql.Identifier(schema)))
            with db.cursor() as cursor:
                cursor.executemany(sql.SQL('INSERT INTO {}.orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)').format(sql.Identifier(schema)),[tuple(row[k] for k in FIELDS) for row in rows()])
        db.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        db.execute(sql.SQL('SET LOCAL search_path TO {}').format(sql.Identifier(schema)))
        snapshot,version=db.execute("SELECT pg_current_snapshot()::text,current_setting('server_version')").fetchone()
        records=[]
        for record in db.execute('SELECT * FROM orders ORDER BY order_id').fetchall():
            row=dict(zip(FIELDS,record));row['ordered_at']=row['ordered_at'].isoformat();records.append(row)
        reports={}
        for name,statement in QUERIES.items():
            cursor=db.execute(statement)
            reports[name]={'columns':[c.name for c in cursor.description],'rows':cursor.fetchall()}
        db.commit()
        source=packet(records,{'engine':'PostgreSQL','version':version,'schema':schema,'snapshot':snapshot,'writes_during_export':'repeatable-read visibility; no replication slot claimed'})
        write_json(work/'source.json',source)
        write_json(work/'source-reports.json',{'boundary':source['boundary'],'queries':QUERIES,'results':reports})
        target=create_table(work,records)
        destination=target_packet(target)
        reconciliation=compare(source,destination,logical_boundary='owned fixture at captured PostgreSQL snapshot')
        consumers=[]
        for name,observed in reports.items():
            actual=query(destination['rows'],QUERIES[name])
            consumers.append({'case':name,'sql':QUERIES[name],'equal':result_equal(observed,actual,ordered=True),'source':observed,'target':actual})
        passed=reconciliation['status']=='passed' and all(c['equal'] for c in consumers)
        result={'status':'passed' if passed else 'failed','source_schema':schema,'reconciliation':reconciliation,'consumer_cases':consumers,'source_deleted':False}
        write_json(work/'source.json',source);write_json(work/'target.json',destination);write_json(work/'postgres.json',result)
        if not passed:raise Refused('Offload differs from the captured source; inspect postgres.json')
        return result
    finally:db.close()
