import copy
from datetime import datetime,timezone
from zoneinfo import ZoneInfo

import pytest

from iceberg_migration.common import Refused,digest
from iceberg_migration.fixture import rows,packet,validate_row
from iceberg_migration.reconcile import compare
from iceberg_migration.boundary import Handoff
from iceberg_migration.cutover import Rehearsal
from iceberg_migration.consumers import result_equal
from iceberg_migration.policy import parity
from iceberg_migration.portfolio import wave,REQUIRED


def test_reconciliation_catches_equal_counts_and_cancelling_totals():
    source=rows();target=copy.deepcopy(source)
    target[0]['refund_cents']+=100;target[1]['refund_cents']-=100
    result=compare(packet(source,{'fixture':1}),packet(target,{'fixture':2}),logical_boundary='same authored image')
    assert result['source_rows']==result['target_rows']==8
    assert result['source_revenue_cents']==result['target_revenue_cents']==39000
    assert result['status']=='failed' and result['missing_rows']==2 and 'row_values' in result['failures']


def test_duplicate_keys_and_changed_packets_are_rejected():
    source=packet(rows(),{'fixture':1});target=packet(rows()+[rows()[0]],{'fixture':2})
    assert 'target_duplicate_key' in compare(source,target,logical_boundary='test')['failures']
    source['rows'][0]['refund_cents']=100
    with pytest.raises(Refused,match='changed after capture'):compare(source,target,logical_boundary='test')


def test_mapping_preserves_null_and_timestamp_meaning():
    row=rows()[0]
    with pytest.raises(Refused):validate_row({**row,'refund_cents':None})
    with pytest.raises(Refused):validate_row({**row,'refund_cents':True})
    with pytest.raises(Refused):validate_row({**row,'ordered_at':'2026-09-01T12:00:00'})
    utc=validate_row({**row,'ordered_at':'2026-09-01T05:00:00-07:00'})
    assert utc==row
    naive=datetime(2026,9,1,12)
    a=naive.replace(tzinfo=timezone.utc)
    b=naive.replace(tzinfo=ZoneInfo('America/Los_Angeles')).astimezone(timezone.utc)
    assert (b-a).total_seconds()==7*3600


def test_consumer_comparison_keeps_duplicates_nulls_and_order():
    a={'columns':['x'],'rows':[(None,),(1,),(1,)]}
    b={'columns':['x'],'rows':[(1,),(None,),(1,)]}
    assert result_equal(a,b) and not result_equal(a,b,ordered=True)
    assert not result_equal(a,{'columns':['x'],'rows':[(None,),(1,)]})
    assert not result_equal(a,{'columns':['renamed'],'rows':a['rows']})


def test_handoff_overlap_replay_gap_and_atomicity(tmp_path):
    store=Handoff(tmp_path/'handoff.sqlite')
    try:
        store.seed(rows(),100)
        first={'position':101,'operation':'upsert','row':{**rows()[0],'refund_cents':100}}
        delete={'position':102,'operation':'delete','key':41002}
        overlap={'position':99,'operation':'delete','key':41001}
        with pytest.raises(RuntimeError):store.apply([first,delete],fail_before_commit=True)
        assert store.image()['applied_position']==100 and store.image()['rows']==rows()
        result=store.apply([overlap,first,delete]);assert result==store.apply([first,delete])
        assert len(result['rows'])==7 and result['rows'][0]['refund_cents']==100
        with pytest.raises(Refused):store.apply([{**first,'operation':'delete','key':41001}])
        with pytest.raises(Refused):store.apply([{'position':104,'operation':'delete','key':41001}])
        assert store.image()==result
    finally:store.close()


def test_cutover_preserves_post_target_writes(tmp_path):
    state=Rehearsal(tmp_path/'cutover.sqlite')
    try:
        state.seed(rows())
        with pytest.raises(Refused):state.route('target')
        state.freeze();state.route('target');state.route('source');state.route('target');state.enable_writer('target')
        with pytest.raises(Refused):state.write('source',rows()[0])
        changed={**rows()[0],'refund_cents':100};state.write('target',changed);state.freeze()
        with pytest.raises(Refused):state.route('source')
        with pytest.raises(Refused):state.recover(approved_target_digest=digest(rows()))
        assert state.image('source')==rows()
        result=state.recover(approved_target_digest=digest(state.image('target')))
        assert result['source_digest']==result['target_digest'] and state.image('source')[0]==changed
        assert state.state()['writer']=='none'
    finally:state.close()


def test_policy_parity_needs_valid_denial_and_full_coverage():
    expected=[{'case_id':'read','identity':'analyst','path':'REST','outcome':'allowed'},
              {'case_id':'deny','identity':'outsider','path':'REST','outcome':'denied'}]
    observed=[expected[0],{**expected[1],'error_category':'authentication'}]
    assert parity(expected,observed)['status']=='failed'
    observed[1]['error_category']='authorization'
    assert parity(expected,observed)['status']=='passed'
    widened=[observed[0],{**observed[1],'outcome':'allowed'}]
    assert parity(expected,widened)['status']=='failed'
    assert parity(expected,observed[:1])['status']=='failed'


def test_portfolio_priority_cannot_bypass_a_gate():
    ready={'table':'orders','owner':'data','consumer_owner':'finance','recovery_owner':'platform','source_boundary':'b1','benefit':10,'complexity':2,
           'evidence':{gate:{'status':'passed','artifact_sha256':digest(gate),'source_boundary':'b1'} for gate in REQUIRED}}
    blocked=copy.deepcopy(ready);blocked['table']='returns';blocked['benefit']=100;blocked['evidence']['policy']['source_boundary']='old'
    result=wave([blocked,ready]);assert result[0]['table']=='orders' and result[0]['eligible']
    assert result[1]['blocked_by']==['policy']
    with pytest.raises(Refused):wave([ready,ready])


def test_benchmark_retains_failure_and_partial_samples(tmp_path):
    from iceberg_migration.benchmark import measure
    from iceberg_migration.common import read_json
    result={'columns':['value'],'rows':[(1,)]}
    calls=0
    def target():
        nonlocal calls
        calls+=1
        if calls==2:raise RuntimeError('authored target failure')
        return result
    path=tmp_path/'timing.json'
    with pytest.raises(RuntimeError):measure(lambda:result,target,trials=3,record=path)
    report=read_json(path)
    assert report['status']=='failed' and len(report['warmup'])==2
    assert len(report['samples'])==2 and report['samples'][-1]['error_type']=='RuntimeError'
    assert report['median_ms']=={}
