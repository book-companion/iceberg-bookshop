"""Authored maintenance decisions for explanation scoring, not observed work."""
from .operations import plan


def cases() -> list[dict]:
    metrics={'table':'agents_fixture.orders','table_uuid':'authored-table','snapshot_id':10,'observed_at':'authored-fixture',
             'data_files':40,'small_files':35,'commit_age_seconds':1000,'retention_hold':True,'small_file_threshold_bytes':128*1024*1024}
    specs=[('over_budget',100,150,False,['rewrite_budget']),
           ('writers_active',100,50,True,['writer_coordination_required']),
           ('bounded_with_hold',100,50,False,[])]
    output=[]
    for key,budget,candidate,writers,expected in specs:
        proposal=plan(metrics,max_rewrite_bytes=budget,candidate_bytes=candidate,concurrent_writers=writers)
        observations=[{'id':key+':metrics','observed_at':'authored-fixture','signal':'observed_metrics','value':metrics},
                      {'id':key+':proposal','observed_at':'authored-fixture','signal':'deterministic_proposal','value':proposal}]
        output.append({'id':key,'observations':observations,'expected':expected})
    return output
