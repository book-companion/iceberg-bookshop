"""Public desktop experiments. Importing this module starts nothing."""
from __future__ import annotations

import argparse
import copy
import json
import os
import time
import uuid
from pathlib import Path

from .common import Refused,canonical,digest,fresh,write_json,read_json
from .fixture import rows,arrow,json_rows,packet,create_table,identity,revenue,target_packet
from .reconcile import compare


def pilot(work):
    import pyarrow.parquet as pq
    from .consumers import query,QUERIES,compare_queries
    from .inventory import scan
    from .benchmark import measure
    source_path=work/'source';source_path.mkdir()
    pq.write_table(arrow(rows()),source_path/'orders.parquet')
    captured=json_rows(pq.read_table(source_path/'orders.parquet'))
    source=packet(captured,{'kind':'immutable authored Parquet fixture','file_sha256':__import__('hashlib').sha256((source_path/'orders.parquet').read_bytes()).hexdigest()})
    inventory=scan(source_path)
    start=time.perf_counter_ns();baseline=query(captured,QUERIES['revenue']);baseline_ms=(time.perf_counter_ns()-start)/1e6
    write_json(work/'inventory.json',inventory)
    write_json(work/'baseline.json',{'result':baseline,'elapsed_ms':baseline_ms,'scope':'one captured-image query including connection and Arrow registration; not a benchmark'})
    write_json(work/'source.json',source)
    target=create_table(work,captured)
    destination=target_packet(target)
    report=compare(source,destination,logical_boundary='immutable fixture before any target change')
    consumers=compare_queries(source['rows'],destination['rows'])
    write_json(work/'source.json',source);write_json(work/'target.json',destination)
    write_json(work/'reconciliation.json',report);write_json(work/'consumers.json',consumers)
    assert report['status']=='passed' and consumers['status']=='passed' and revenue(destination['rows'])==39000
    timing=measure(lambda:query(source['rows'],QUERIES['revenue']),lambda:query(destination['rows'],QUERIES['revenue']),record=work/'timing.json')
    write_json(work/'timing.json',timing)
    return {'status':'passed','reconciliation':report,'consumer_cases':len(consumers['cases']),
            'scope':'local Parquet to real Iceberg V2; no remote policy or production performance claim'}


def raw(work):
    from .parquet import seed_zone,normalize
    seed_zone(work/'zone');data,report=normalize(work/'zone',work/'normalized')
    assert len(data)==8 and report['quarantined_files']==2 and report['release_eligible'] is False
    target=create_table(work,data)
    reconciliation=compare(packet(data,{'accepted_manifest':digest(report)}),target_packet(target),logical_boundary='accepted subset only; two files remain quarantined')
    assert reconciliation['status']=='passed'
    result={'status':'passed','scope':'quarantine behavior and accepted-subset copy; whole source release remains blocked','normalization':report,'reconciliation':reconciliation}
    write_json(work/'raw.json',result);return result


def boundary(work):
    from .boundary import Handoff
    baseline=rows();updated={**baseline[0],'refund_cents':100};inserted={**baseline[-1],'order_id':41999}
    events=[{'position':101,'operation':'upsert','row':updated},{'position':102,'operation':'delete','key':41002},{'position':103,'operation':'upsert','row':inserted}]
    store=Handoff(work/'handoff.sqlite')
    try:
        store.seed(baseline,100)
        try:store.apply(events,fail_before_commit=True)
        except RuntimeError:pass
        assert store.image()['applied_position']==100 and store.image()['rows']==baseline
        result=store.apply(events);assert result==store.apply(events)
        expected=sorted([updated]+[r for r in baseline[1:] if r['order_id']!=41002]+[inserted],key=lambda r:r['order_id'])
        assert result['rows']==expected
        gap_refused=False
        try:store.apply([{'position':105,'operation':'delete','key':41003}])
        except Refused:gap_refused=True
        assert gap_refused and store.image()['applied_position']==103
        output={'status':'passed','backfill_boundary':100,'applied_position':103,'replay_idempotent':True,'gap_refused':True,
                'source_event_model':'authored contiguous single-stream integers; not PostgreSQL LSN or Kafka offsets'}
        write_json(work/'events.json',events);write_json(work/'boundary.json',output);return output
    finally:store.close()


def cutover(work):
    from .cutover import Rehearsal
    rehearsal=Rehearsal(work/'cutover.sqlite')
    try:
        rehearsal.seed(rows());rehearsal.freeze();rehearsal.route('target')
        rehearsal.route('source')
        rehearsal.route('target');rehearsal.enable_writer('target')
        changed={**rows()[0],'refund_cents':100};rehearsal.write('target',changed);rehearsal.freeze()
        refused=False
        try:rehearsal.route('source')
        except Refused:refused=True
        assert refused
        recovered=rehearsal.recover(approved_target_digest=digest(rehearsal.image('target')))
        assert recovered['source_digest']==recovered['target_digest']
        rehearsal.enable_writer('source')
        result={'status':'passed','before_target_write_switchback':True,'after_target_write_shortcut_refused':True,
                'recovery':recovered,'state':rehearsal.state()}
        write_json(work/'cutover.json',result);return result
    finally:rehearsal.close()


def target(work):
    from .spark import session,frame,collect
    from .policy import catalog
    spark=session(work,rest=True);ns='migr_'+uuid.uuid4().hex[:12]
    try:
        spark.sql(f'CREATE NAMESPACE ice.{ns}')
        frame(spark).writeTo(f'ice.{ns}.orders').using('iceberg').tableProperty('format-version','2').create()
        table=catalog(os.environ['MIGR_CREDENTIAL']).load_table(ns+'.orders')
        report=compare(packet(rows(),{'fixture':'authored eight orders'}),target_packet(table),logical_boundary='quiescent fresh Spark fixture read by PyIceberg')
        assert report['status']=='passed'
        result={'status':'passed','table':ns+'.orders','reconciliation':report,'policy_denial':'separate policy scenario required'}
        write_json(work/'target-readiness.json',result);return result
    finally:spark.stop()


def layout(work):
    from .spark import session,frame,collect
    from .benchmark import measure
    spark=session(work);ns='migr_'+uuid.uuid4().hex[:12]
    try:
        spark.sql(f'CREATE NAMESPACE ice.{ns}')
        base=rows();scaled=[{**r,'order_id':r['order_id']+batch*100000} for batch in range(500) for r in base]
        frame(spark,scaled).createOrReplaceTempView('migration_input')
        spark.sql(f'CREATE TABLE ice.{ns}.plain USING iceberg AS SELECT * FROM migration_input')
        spark.sql(f'CREATE TABLE ice.{ns}.by_day USING iceberg PARTITIONED BY (days(ordered_at)) AS SELECT * FROM migration_input')
        spark.sql(f'CREATE TABLE ice.{ns}.by_day_sorted USING iceberg PARTITIONED BY (days(ordered_at)) AS SELECT * FROM migration_input WHERE false')
        spark.sql(f'ALTER TABLE ice.{ns}.by_day_sorted WRITE ORDERED BY tenant, order_id')
        spark.sql(f'INSERT INTO ice.{ns}.by_day_sorted SELECT * FROM migration_input')
        expected=sorted(scaled,key=lambda row:row['order_id'])
        for name in ('plain','by_day','by_day_sorted'):
            assert collect(spark.table(f'ice.{ns}.{name}'))==expected,'Layout changed business rows'
        def execute(name):
            result=spark.sql(f"SELECT tenant, sum(gross_cents-discount_cents-refund_cents) AS revenue_cents FROM ice.{ns}.{name} WHERE status='paid' AND ordered_at >= TIMESTAMP '2026-09-01 00:00:00' AND ordered_at < TIMESTAMP '2026-09-02 00:00:00' GROUP BY tenant ORDER BY tenant")
            return {'columns':result.columns,'rows':[tuple(r) for r in result.collect()]}
        report=measure(lambda:execute('plain'),lambda:execute('by_day'),record=work/'partition-timing.json')
        def point_query(name):
            result=spark.sql(f"SELECT order_id,status,refund_cents FROM ice.{ns}.{name} WHERE tenant='north' AND order_id >= 41001 AND order_id < 1041001 ORDER BY order_id")
            return {'columns':result.columns,'rows':[tuple(r) for r in result.collect()]}
        sort_report=measure(lambda:point_query('by_day'),lambda:point_query('by_day_sorted'),record=work/'sort-timing.json')
        metadata={name:[r.asDict() for r in spark.sql(f'SELECT count(*) AS files,sum(file_size_in_bytes) AS bytes FROM ice.{ns}.{name}.files').collect()] for name in ('plain','by_day','by_day_sorted')}
        for name in ('plain','by_day','by_day_sorted'):
            plan=spark.sql(f"EXPLAIN FORMATTED SELECT * FROM ice.{ns}.{name} WHERE ordered_at >= TIMESTAMP '2026-09-01 00:00:00' AND ordered_at < TIMESTAMP '2026-09-02 00:00:00'").collect()
            (work/(name+'-plan.txt')).write_text('\n'.join(str(r[0]) for r in plan)+'\n')
        result={'status':'passed','namespace':ns,'rows':len(scaled),'timing':report,'sort_timing':sort_report,'files':metadata,
                'configured_sort':'tenant ASC, order_id ASC on by_day_sorted; query ordering remains explicit',
                'scope':'small local Spark layout comparison, no production speedup or cloud price claim'}
        write_json(work/'layout.json',result);return result
    finally:spark.stop()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--scenario',required=True,choices=['pilot','raw','boundary','cutover','target','layout','policy','postgres','hive','delta','hudi','xtable','cdc','cdc-prepare','cdc-live'])
    parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--source-path',type=Path);parser.add_argument('--source-format',choices=['DELTA','HUDI']);parser.add_argument('--expected',type=Path)
    parser.add_argument('--manifest',type=Path)
    args=parser.parse_args();work=args.work.resolve();fresh(work)
    try:
        if args.scenario in ('pilot','raw','boundary','cutover','target','layout'):result=globals()[args.scenario](work)
        elif args.scenario=='policy':
            from .policy import controls
            result=controls()
            write_json(work/'policy-observed.json',result['observations'])
        elif args.scenario=='postgres':
            from .postgres import offload
            result=offload(work)
        elif args.scenario=='hive':
            from .hive import exercise
            result=exercise(work)
        elif args.scenario in ('delta','hudi'):
            from .formats import feasibility
            result=feasibility(work,args.scenario)
        elif args.scenario=='xtable':
            from .formats import xtable
            if not all((args.source_path,args.source_format,args.expected)):raise Refused('XTable needs --source-path, --source-format and --expected captured source packet')
            result=xtable(work,source_path=args.source_path,source_format=args.source_format,expected=read_json(args.expected))
        elif args.scenario=='cdc-prepare':
            from .cdc_live import prepare
            result=prepare(work)
        elif args.scenario=='cdc-live':
            from .cdc_live import exercise
            if not args.manifest:raise Refused('Provide the cdc-prepare manifest after starting its owned connector and job')
            result=exercise(work,args.manifest)
        else:
            from .cdc_bridge import existing_convergence
            result=existing_convergence(work)
        write_json(work/'result.json',result);print(canonical(result))
    except Refused as exc:
        # Prerequisite and application refusals are retained, never successful runs.
        print(canonical({'status':'failed','reason':str(exc)}));raise SystemExit(2)


if __name__=='__main__':main()
