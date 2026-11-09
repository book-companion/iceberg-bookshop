"""Evidence-gated wave planning; no execution or approval capability."""
from .common import Refused, digest
from .inventory import readiness

REQUIRED=('data','consumers','policy','sync','recovery','ownership')


def wave(items):
    names=[item['table'] for item in items]
    if len(names)!=len(set(names)):raise Refused('Duplicate portfolio table')
    result=[]
    for item in items:
        ready=readiness(item);reasons=list(ready['missing'])
        evidence=item.get('evidence',{})
        for gate in REQUIRED:
            record=evidence.get(gate,{})
            if record.get('status')!='passed' or not record.get('artifact_sha256') or record.get('source_boundary')!=item.get('source_boundary'):
                reasons.append(gate)
        result.append({'table':item['table'],'eligible':not reasons,'blocked_by':sorted(set(reasons)),
                       'priority_score':ready['priority_score'],'input_sha256':digest(item),
                       'authorization':'planning only; claimed evidence must be independently reviewed'})
    return sorted(result,key=lambda r:(not r['eligible'],-r['priority_score'],r['table']))
