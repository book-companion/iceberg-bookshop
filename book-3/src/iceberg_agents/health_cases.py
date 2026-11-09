"""Authored diagnostic cases and the Book 2 rule thresholds, not measurements."""
from __future__ import annotations

from .contracts import Refused


def pack_findings(row: dict) -> dict:
    """Preserve unknown counters; do not convert a failed measurement to zero."""
    checks=[('small_files',('excess_files','median_file_kb'),lambda r:r['excess_files']>20 and r['median_file_kb']<32768),
            ('delete_debt',('deletes_per_file','delete_files'),lambda r:r['deletes_per_file']>100 or r['delete_files']>10),
            ('compaction_lag',('since_compaction',),lambda r:r['since_compaction']>50),
            ('metadata_bloat',('metadata_kb',),lambda r:r['metadata_kb']>256),
            ('manifest_sprawl',('manifests',),lambda r:r['manifests']>20),
            ('stale_stream',('class','freshness_min'),lambda r:r['class']=='stream' and r['freshness_min']>5)]
    findings=[];unknown=[]
    for name,fields,condition in checks:
        if any(row.get(field) is None for field in fields):unknown.append(name)
        elif condition(row):findings.append(name)
    return {'findings':findings,'unknown':unknown,'rule_version':'book2-health-rules-1'}


def cases() -> list[dict]:
    healthy={'class':'batch','excess_files':0,'median_file_kb':65536,'deletes_per_file':0,'delete_files':0,
             'since_compaction':1,'metadata_kb':32,'manifests':1,'freshness_min':120}
    return [
        {'id':'quiet_batch','metrics':healthy,'expected':[]},
        {'id':'stale_stream','metrics':{**healthy,'class':'stream'},'expected':['stale_stream']},
        {'id':'fragmented','metrics':{**healthy,'excess_files':35,'median_file_kb':4,'since_compaction':60},'expected':['small_files','compaction_lag']},
        {'id':'delete_pressure','metrics':{**healthy,'delete_files':12,'deletes_per_file':120,'manifests':25},'expected':['delete_debt','manifest_sprawl']},
        {'id':'unknown_metadata','metrics':{**healthy,'metadata_kb':None},'expected':[],'unknown':['metadata_bloat']},
    ]


def observations(case: dict) -> list[dict]:
    rows = [{'id':case['id']+':'+key,'observed_at':'authored-fixture','signal':key,'value':value}
            for key,value in case['metrics'].items()]
    rows.append({'id':case['id']+':rules','observed_at':'authored-fixture','signal':'deterministic_findings','value':pack_findings(case['metrics'])})
    return rows
