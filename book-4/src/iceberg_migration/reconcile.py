"""Exact small-pilot comparison; full mismatches counted, examples bounded."""
from __future__ import annotations

from collections import Counter

from .common import Refused, canonical, digest, now
from .fixture import validate_row, revenue


def compare(source, target, *, logical_boundary: str, limit=20):
    if not logical_boundary or not source.get('boundary') or not target.get('boundary'):
        raise Refused('Both physical identities and the declared logical boundary are required')
    if type(limit) is not int or limit<1:
        raise Refused('Mismatch example limit must be positive')
    for packet in (source,target):
        if packet.get('rows_sha256')!=digest(packet['rows']):
            raise Refused('Packet rows changed after capture')
    left,right=source['rows'],target['rows']
    failures=[]
    if source['schema']!=target['schema']:failures.append('schema')
    normalized=[]
    for label,data in (('source',left),('target',right)):
        try: values=[validate_row(row) for row in data]
        except (ValueError,TypeError,KeyError):
            failures.append(label+'_mapping');values=data
        normalized.append(values)
        keys=[canonical(r.get('order_id')) for r in values]
        if len(keys)!=len(set(keys)):failures.append(label+'_duplicate_key')
    a,b=map(lambda data:Counter(canonical(r) for r in data),normalized)
    missing=list((a-b).elements());extra=list((b-a).elements())
    if len(left)!=len(right):failures.append('row_count')
    if missing or extra:failures.append('row_values')
    totals={}
    if not any(f.endswith('_mapping') for f in failures):
        totals={'source_revenue_cents':revenue(normalized[0]),'target_revenue_cents':revenue(normalized[1])}
        if totals['source_revenue_cents']!=totals['target_revenue_cents']:failures.append('business_total')
    return {'status':'passed' if not failures else 'failed','observed_at':now(),'failures':failures,
            'logical_boundary':logical_boundary,'boundary_assertion':'operator-declared; compare does not establish source consistency',
            'source_boundary':source['boundary'],'target_boundary':target['boundary'],
            'source_rows':len(left),'target_rows':len(right),'missing_rows':len(missing),'extra_rows':len(extra),
            'missing_examples':missing[:limit],'extra_examples':extra[:limit],**totals,
            'source_packet_sha256':digest(source),'target_packet_sha256':digest(target)}
