#!/usr/bin/env python3
"""Actual local/MCP/REST/Spark probes. All are unrun at draft delivery."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
import uuid
import subprocess


def versions():
    from importlib.metadata import version, PackageNotFoundError
    result={}
    for name in ('pyiceberg','pyspark','duckdb','pyarrow','mcp','claude-agent-sdk','sqlglot'):
        try:result[name]=version(name)
        except PackageNotFoundError:result[name]=None
    return result


async def protocol(work:Path):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from iceberg_agents.fixture import seed
    os.environ['BOOK3_CATALOG']='local'
    os.environ['BOOK3_TABLE']='agents_demo.orders'
    seed(work)
    env={k:v for k,v in os.environ.items() if k.startswith('BOOK3_')}
    env.pop('BOOK3_RUN_ID',None)
    env.update(BOOK3_PRINCIPAL='north_analyst',BOOK3_WORK=str(work),BOOK3_TOOL_BUDGET='3')
    params=StdioServerParameters(command=sys.executable,args=['-m','iceberg_agents.server'],env=env)
    async with stdio_client(params) as (read,write):
        async with ClientSession(read,write) as client:
            await client.initialize()
            tools=await client.list_tools()
            names={tool.name for tool in tools.tools}
            assert {'discover','describe_orders','query_metric','query_sql'}<=names
            assert not {'publish','approve','stage','shell'}&names
            def value(result):
                return result.structuredContent or json.loads(result.content[0].text)
            semantic=value(await client.call_tool('describe_orders',{}));assert semantic['ok']
            answer=value(await client.call_tool('query_metric',{'metric':'net_revenue','start':'2026-09-01','end':'2026-10-01'}))
            assert answer['ok'] and answer['value']['rows']==[{'net_revenue_cents':21000}]
            denied=value(await client.call_tool('query_sql',{'sql':"SELECT * FROM read_csv('/etc/passwd')"}))
            assert not denied['ok']
            budget=value(await client.call_tool('discover',{}))
            assert budget['code']=='tool_budget'
            events=[json.loads(line) for line in (work/'mcp-trace.jsonl').read_text().splitlines()]
            assert len(events)==4 and events[-1]['status']=='tool_budget'
            assert events[1]['receipt_id']==answer['value']['request_id']
            assert '/etc/passwd' not in (work/'mcp-trace.jsonl').read_text()
            return {'tools':sorted(names),'north_revenue_cents':21000,'file_read_refused':True,'call_budget_refused':True}


def require_rest():
    needed=['BOOK3_REST_URI','BOOK3_WAREHOUSE','BOOK3_CREDENTIAL','BOOK3_S3_ENDPOINT']
    if any(not os.environ.get(k) for k in needed):
        raise RuntimeError('Missing desktop REST/operator configuration; see DESKTOP.md')
    os.environ['BOOK3_CATALOG']='rest'


def book2_health(work:Path):
    from iceberg_agents.health_cases import pack_findings
    book=Path(__file__).resolve().parents[2]
    python=book.parent/'.venv'/'bin'/'python'
    if not python.is_file():raise RuntimeError('Prepare Book 2\'s root virtual environment before running its health pack')
    namespaces=os.environ.get('BOOK3_HEALTH_NAMESPACES','bookshop').split(',')
    output=work/'book2-health'
    result=subprocess.run([str(python),str(book/'health_pack_bridge.py'),*namespaces,'--output',str(output)],
                          cwd=book,capture_output=True,text=True,timeout=300)
    if result.returncode:
        raise AssertionError('Book 2 health pack failed; rerun the public bridge directly for operator diagnostics')
    packet=json.loads((output/'raw.json').read_text())
    if not packet['rows']:raise AssertionError('No observed table is not a passing health scan')
    reports=[{'table':row['tbl'],**pack_findings(row)} for row in packet['rows']]
    return {'tables':len(reports),'reports':reports,'observations':str(output/'observations.json'),
            'unknown_measurements':sum(len(row['unknown']) for row in reports)}


def catalog_identity(work:Path):
    """A valid restricted credential must load its control table before refusal."""
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import ForbiddenError
    needed=['BOOK3_RESTRICTED_CREDENTIAL','BOOK3_ALLOWED_CONTROL','BOOK3_FORBIDDEN_CONTROL']
    if any(not os.environ.get(k) for k in needed):raise RuntimeError('Provision the restricted principal and two control tables first')
    from iceberg_agents.catalog import catalog
    admin=catalog(work)
    allowed=admin.load_table(os.environ['BOOK3_ALLOWED_CONTROL'])
    forbidden=admin.load_table(os.environ['BOOK3_FORBIDDEN_CONTROL'])
    restricted=load_catalog('restricted',type='rest',uri=os.environ['BOOK3_REST_URI'],warehouse=os.environ['BOOK3_WAREHOUSE'],
                            credential=os.environ['BOOK3_RESTRICTED_CREDENTIAL'],scope=os.environ.get('BOOK3_SCOPE','PRINCIPAL_ROLE:ALL'),
                            **{'s3.endpoint':os.environ['BOOK3_S3_ENDPOINT']})
    control=restricted.load_table(os.environ['BOOK3_ALLOWED_CONTROL'])
    assert control.scan(limit=1).to_arrow().num_rows==1, 'The allowed control must contain a known readable row'
    try:restricted.load_table(os.environ['BOOK3_FORBIDDEN_CONTROL'])
    except ForbiddenError:
        return {'control_table_uuid':str(allowed.metadata.table_uuid),'forbidden_table_uuid':str(forbidden.metadata.table_uuid),'catalog_refusal':'ForbiddenError'}
    raise AssertionError('Forbidden table load succeeded; a missing table or dead catalog is not an acceptable denial proof')


def stack(work:Path,scenario:str):
    require_rest()
    if scenario=='identity':return catalog_identity(work)
    from iceberg_agents.sparkops import session,seed_rest,SparkBridge,lineage_image
    from iceberg_agents.gateway import Gateway
    from iceberg_agents.contracts import MetricRequest,Refused
    from iceberg_agents.health import observe
    from iceberg_agents.workflow import Workflow,validate_change
    from iceberg_agents.retrieval import Index, search_current
    from iceberg_agents.operations import assess
    # Fresh sandbox for every attempt. No shared bookshop table is overwritten.
    os.environ['BOOK3_TABLE']='agents_probe_'+uuid.uuid4().hex[:12]+'.orders'
    spark=session();ledger=None;index=None
    try:
        seed_rest(spark,format_version=3 if scenario=='lineage' else 2)
        bridge=SparkBridge(spark,work)
        original=bridge.identity()
        if scenario in ('platform','health'):
            receipt=Gateway(work,'finance').metric(MetricRequest('net_revenue'))
            assert receipt.rows==[{'net_revenue_cents':39000}]
            metrics=observe(work)
            assert metrics['snapshot_id']==original['snapshot_id'] and metrics['data_files']>=1
            return {'identity':original,'revenue_cents':39000,'metrics':metrics,'findings':assess(metrics)}
        if scenario=='lineage':
            index=Index(work/'retrieval.sqlite')
            first=lineage_image(spark,work);index.sync(**first)
            current=search_current(work,'north_analyst','paid order')
            assert all(row['tenant']=='north' for row in current['documents'])
            spark.sql(f"UPDATE {bridge.qualified} SET status='refunded' WHERE order_id=41001")
            spark.sql(f"DELETE FROM {bridge.qualified} WHERE order_id=41002")
            try:search_current(work,'north_analyst','paid order')
            except Refused as exc:assert exc.code=='stale_index'
            else:raise AssertionError('A stale index was returned as current')
            second=lineage_image(spark,work)
            report=index.sync(**second)
            assert report['deleted']==1
            assert index.db.execute('SELECT count(*) FROM documents').fetchone()[0]==7
            assert any('refunded' in row['text'] for row in index.search('refunded',('north',),20))
            assert index.sync(**second)['replayed']
            assert search_current(work,'north_analyst','refunded')['snapshot_id']==second['snapshot_id']
            return {'before':first,'after':second,'sync':report}
        if scenario=='publication':
            ledger=Workflow(work/'approvals.sqlite')
            proposal=ledger.propose(table=bridge.table,table_uuid=original['table_uuid'],base_snapshot=original['snapshot_id'],proposer='writer',order_id=41001,refund_cents=100)
            key=proposal['change_id']
            ledger.stage_with_bridge(key,bridge);candidate=ledger.get(key)['candidate_snapshot']
            assert bridge.identity()['snapshot_id']==original['snapshot_id']
            ledger.validate_with_bridge(key,bridge)
            try:ledger.publish(key,bridge)
            except Refused as exc:assert exc.code=='approval_refused'
            else:raise AssertionError('Publication succeeded without approval')
            ledger.approve(key,'reviewer');published=ledger.publish(key,bridge)
            assert published['state']=='published' and bridge.identity()['snapshot_id']==candidate
            assert ledger.publish(key,bridge)['state']=='published'
            final=Gateway(work,'finance').metric(MetricRequest('net_revenue'))
            assert final.rows==[{'net_revenue_cents':38900}]
            # A second proposal becomes stale when an independent main write wins.
            p2=ledger.propose(table=bridge.table,table_uuid=original['table_uuid'],base_snapshot=candidate,proposer='writer',order_id=41001,refund_cents=200)
            ledger.stage_with_bridge(p2['change_id'],bridge);c2=ledger.get(p2['change_id'])['candidate_snapshot']
            ledger.validate_with_bridge(p2['change_id'],bridge)
            ledger.approve(p2['change_id'],'reviewer')
            spark.sql(f"UPDATE {bridge.qualified} SET refund_cents=50 WHERE order_id=41006")
            try:ledger.publish(p2['change_id'],bridge)
            except Refused as exc:assert exc.code=='base_changed'
            else:raise AssertionError('Stale approval published after main changed')
            return {'original':original,'published':published,'stale_approval_refused':True,'sandbox_table':bridge.table}
        raise ValueError('Unknown stack scenario')
    finally:
        if ledger:ledger.close()
        if index:index.close()
        spark.stop()


def main():
    p=argparse.ArgumentParser();p.add_argument('--scenario',choices=['protocol','platform','identity','health','book2-health','lineage','publication','failures'],required=True);p.add_argument('--work',type=Path,required=True)
    args=p.parse_args();print(json.dumps({'versions':versions()}),flush=True)
    try:
        if args.scenario=='failures':
            from iceberg_agents.failure_drills import run
            result=run(args.work)
        else:
            result=book2_health(args.work) if args.scenario=='book2-health' else (asyncio.run(protocol(args.work)) if args.scenario=='protocol' else stack(args.work,args.scenario))
        print(json.dumps(result,default=str,sort_keys=True));return 0
    except (ImportError,RuntimeError) as exc:
        print('BLOCKED:',str(exc),file=sys.stderr);return 3


if __name__=='__main__':raise SystemExit(main())
