"""Desktop local-integration tests: real PyIceberg, Arrow and DuckDB; no Docker."""
from dataclasses import asdict

import pytest

from iceberg_agents.fixture import seed, local_catalog, records, arrow_schema
from iceberg_agents.gateway import Gateway
from iceberg_agents.contracts import MetricRequest, Refused
from iceberg_agents.provenance import retain, replay


def test_authorized_totals_and_denial(tmp_path):
    seed(tmp_path)
    total=Gateway(tmp_path,'finance').metric(MetricRequest('net_revenue'))
    north=Gateway(tmp_path,'north_analyst').metric(MetricRequest('net_revenue'))
    assert total.rows==[{'net_revenue_cents':39000}]
    assert north.rows==[{'net_revenue_cents':21000}]
    with pytest.raises(Refused):Gateway(tmp_path,'outsider').metric(MetricRequest('net_revenue'))
    assert total.table_uuid==north.table_uuid
    assert Gateway(tmp_path,'finance').metric(MetricRequest('active_customers')).rows==[{'active_customers':5}]
    assert Gateway(tmp_path,'north_analyst').metric(MetricRequest('active_customers')).rows==[{'active_customers':3}]


def test_semantic_end_date_and_duplicate_grain(tmp_path):
    import pyarrow as pa
    from datetime import date
    seed(tmp_path)
    table=local_catalog(tmp_path).load_table('agents_demo.orders')
    october=records()[0];october.update(order_id=43000,order_date=date(2026,10,1),gross_cents=100,discount_cents=0,refund_cents=0)
    table.append(pa.Table.from_pylist([october],schema=arrow_schema()))
    gateway=Gateway(tmp_path,'finance')
    assert gateway.metric(MetricRequest('net_revenue')).rows==[{'net_revenue_cents':39000}]
    assert gateway.metric(MetricRequest('net_revenue','2026-10-01','2026-11-01')).rows==[{'net_revenue_cents':100}]
    table.append(pa.Table.from_pylist([records()[0]],schema=arrow_schema()))
    with pytest.raises(Refused,match='one row per order'):gateway.metric(MetricRequest('net_revenue'))


def test_replay_uses_recorded_snapshot(tmp_path):
    import pyarrow as pa
    seed(tmp_path)
    receipt=asdict(Gateway(tmp_path,'finance').metric(MetricRequest('net_revenue')))
    tag=retain(tmp_path,receipt)
    table=local_catalog(tmp_path).load_table('agents_demo.orders')
    extra=records()[0];extra['order_id']=42000;extra['gross_cents']=100
    extra['discount_cents']=0;extra['refund_cents']=0
    table.append(pa.Table.from_pylist([extra],schema=arrow_schema()))
    current=Gateway(tmp_path,'finance').metric(MetricRequest('net_revenue'))
    assert current.rows==[{'net_revenue_cents':39100}]
    assert replay(tmp_path,'finance',receipt)['matches']
    assert local_catalog(tmp_path).load_table('agents_demo.orders').metadata.refs[tag].snapshot_id==receipt['snapshot_id']
    with pytest.raises(Refused):replay(tmp_path,'north_analyst',receipt)


def test_trace_lake_tracks_ack_without_duplicate_run(tmp_path):
    from iceberg_agents.runstore import RunStore
    from iceberg_agents.trace_lake import ingest
    store=RunStore(tmp_path/'runs.sqlite')
    output=tmp_path/'export.jsonl'
    try:
        store.start('trace-one','finance','bounded')
        store.finish('trace-one',{'result':'fixture'},latency_ms=10)
        store.export(output);ingest(tmp_path,output)
        stale=tmp_path/'stale.jsonl';stale.write_text(output.read_text())
        store.acknowledge('trace-one');store.export(output);ingest(tmp_path,output)
        ingest(tmp_path,output)
        with pytest.raises(Refused,match='older'):ingest(tmp_path,stale)
        rows=local_catalog(tmp_path).load_table('agents_traces.runs').scan().to_arrow().to_pylist()
        assert len(rows)==1 and rows[0]['delivered']==1
        assert rows[0]['revision']==2
        assert 'result_json' not in rows[0]
    finally:store.close()


def test_judgments_preserve_versions_and_require_runs(tmp_path):
    import json
    from iceberg_agents.runstore import RunStore
    from iceberg_agents.trace_lake import ingest
    from iceberg_agents.judgments import ingest_judgments
    store=RunStore(tmp_path/'runs.sqlite')
    try:
        store.start('judged-run','finance','bounded');store.finish('judged-run',{'value':39000})
        source=tmp_path/'runs.jsonl';store.export(source);ingest(tmp_path,source)
        store.record_tool('judged-run','call-one','query_metric','completed','receipt-one')
        tool_source=tmp_path/'tools.jsonl';store.export_tools(tool_source)
    finally:store.close()
    assert ingest_judgments(tmp_path,tool_source,'tool_calls')['inserted']==1
    assert ingest_judgments(tmp_path,tool_source,'tool_calls')['replayed']==1
    evaluation={'evaluation_id':'e1','run_id':'judged-run','case_id':'finance-revenue','evaluator_version':'oracle-v1','verdict':'pass','reason_code':'value_matches','observed_at':'fixture-time'}
    path=tmp_path/'evaluations.jsonl';path.write_text(json.dumps(evaluation)+'\n')
    assert ingest_judgments(tmp_path,path,'evaluations')['inserted']==1
    assert ingest_judgments(tmp_path,path,'evaluations')['replayed']==1
    evaluation['verdict']='fail';path.write_text(json.dumps(evaluation)+'\n')
    with pytest.raises(Refused,match='new id'):ingest_judgments(tmp_path,path,'evaluations')
    evaluation.update(evaluation_id='e2',evaluator_version='rubric-v2');path.write_text(json.dumps(evaluation)+'\n')
    assert ingest_judgments(tmp_path,path,'evaluations')['inserted']==1
    feedback={'feedback_id':'f1','run_id':'judged-run','reporter':'fixture-reviewer','disposition':'unclear','reason_code':'scope_not_explained','observed_at':'fixture-time'}
    path.write_text(json.dumps(feedback)+'\n');assert ingest_judgments(tmp_path,path,'feedback')['inserted']==1
    feedback.update(feedback_id='f2',run_id='absent');path.write_text(json.dumps(feedback)+'\n')
    with pytest.raises(Refused,match='absent'):ingest_judgments(tmp_path,path,'feedback')


@pytest.mark.parametrize('race_target',[None,'main','candidate'])
def test_publication_checks_refs_at_commit(tmp_path,monkeypatch,race_target):
    import pyarrow as pa
    from iceberg_agents.publication import fast_forward_exact
    from pyiceberg.exceptions import CommitFailedException
    seed(tmp_path)
    cat=local_catalog(tmp_path);table=cat.load_table('agents_demo.orders')
    base=table.current_snapshot().snapshot_id;identity=str(table.metadata.table_uuid)
    branch='review_'+'a'*32
    with table.manage_snapshots() as changes:changes.create_branch(base,branch)
    extra=records()[0];extra['order_id']=44000
    table.append(pa.Table.from_pylist([extra],schema=arrow_schema()),branch=branch)
    candidate=cat.load_table('agents_demo.orders').metadata.refs[branch].snapshot_id
    if race_target:
        class RacingCatalog:
            def load_table(self,name):return cat.load_table(name)
            def commit_table(self,table,requirements,updates):
                rival=cat.load_table('agents_demo.orders')
                row=records()[0];row['order_id']=45000
                rival.append(pa.Table.from_pylist([row],schema=arrow_schema()),branch='main' if race_target=='main' else branch)
                return cat.commit_table(table,requirements,updates)
        monkeypatch.setattr('iceberg_agents.publication.catalog',lambda work:RacingCatalog())
        with pytest.raises(CommitFailedException):
            fast_forward_exact(tmp_path,'agents_demo.orders',branch,expected_uuid=identity,expected_base=base,expected_candidate=candidate)
        assert cat.load_table('agents_demo.orders').current_snapshot().snapshot_id!=candidate
    else:
        fast_forward_exact(tmp_path,'agents_demo.orders',branch,expected_uuid=identity,expected_base=base,expected_candidate=candidate)
        assert cat.load_table('agents_demo.orders').current_snapshot().snapshot_id==candidate


def test_sql_alias_cannot_poison_metric_evidence(tmp_path,monkeypatch):
    from iceberg_agents.agent import validate_answer
    monkeypatch.setenv('BOOK3_RUN_ID','poison-case')
    seed(tmp_path)
    gateway=Gateway(tmp_path,'finance')
    receipt=gateway.query('SELECT 39000 AS net_revenue_cents FROM orders LIMIT 1')
    answer={'status':'answer','answer':'fixture explanation','value':39000,'unit':'USD cents','receipt_ids':[receipt.request_id]}
    with pytest.raises(Refused):validate_answer(answer,tmp_path,'poison-case','finance')
    approved=gateway.metric(MetricRequest('net_revenue'))
    answer['receipt_ids']=[approved.request_id]
    validate_answer(answer,tmp_path,'poison-case','finance')
    with pytest.raises(Refused):validate_answer(answer,tmp_path,'poison-case','north_analyst')
