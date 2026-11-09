"""Isolated local failure experiments; never stop shared platform services."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from unittest.mock import patch

from .contracts import MetricRequest, Refused, write_json
from .fixture import seed
from .gateway import Gateway


@contextmanager
def environment(**values):
    previous={key:os.environ.get(key) for key in values}
    os.environ.update(values)
    try:yield
    finally:
        for key,value in previous.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value


def run(work: Path) -> dict:
    results=[]
    def refused(name,operation,allowed):
        tick=time.monotonic()
        try:operation()
        except Refused as exc:
            assert exc.code in allowed,(name,exc.code)
            results.append({'case':name,'outcome':exc.code,'elapsed_seconds':time.monotonic()-tick})
        else:raise AssertionError(name+' unexpectedly returned success')
    with environment(BOOK3_CATALOG='local',BOOK3_TABLE='agents_demo.orders'):
        seed(work)
        gateway=Gateway(work,'finance')
        assert gateway.metric(MetricRequest('net_revenue')).rows==[{'net_revenue_cents':39000}]
        refused('semantic_contract',lambda:gateway.metric(MetricRequest('unapproved_metric')),{'unknown_metric'})
        refused('engine_parameter_binding',lambda:gateway.query('SELECT order_id FROM orders WHERE order_id = ?',[]),{'engine_failed'})
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1',0))
            port=reserved.getsockname()[1]
            # Reserved but not listening: only this fixture endpoint is affected.
            with environment(BOOK3_CATALOG='rest',BOOK3_REST_URI=f'http://127.0.0.1:{port}/api/catalog',
                             BOOK3_WAREHOUSE='unavailable-fixture',BOOK3_CREDENTIAL='fixture-client:fixture-secret'):
                refused('catalog_unavailable',lambda:gateway.metric(MetricRequest('net_revenue')),{'catalog_failed','query_timeout'})
        original_run=subprocess.run
        def sleeping_worker(command,**kwargs):
            return original_run([sys.executable,'-c','import time; time.sleep(60)'],**kwargs)
        bounded=Gateway(work,'finance');bounded.principal=replace(bounded.principal,timeout_seconds=1)
        with patch('iceberg_agents.gateway.subprocess.run',sleeping_worker):
            refused('actual_worker_deadline',lambda:bounded.metric(MetricRequest('net_revenue')),{'query_timeout'})
        assert gateway.metric(MetricRequest('net_revenue')).rows==[{'net_revenue_cents':39000}]
    report={'cases':results,'before_after_control':'39000 cents','shared_services_stopped':False,
            'semantic_service_note':'The semantic definition is in-process; this tests its contract refusal, not a nonexistent remote service outage',
            'engine_note':'Actual engine argument failure; the timeout separately substitutes a disposable sleeping worker'}
    write_json(work/'failure-drills.json',report)
    return report
