"""Warm, interleave, retain all samples; measure actual execution only when run."""
from __future__ import annotations

import statistics
import time

from .common import Refused,write_json
from .consumers import result_equal


def measure(source,target,*,trials=7,record=None):
    if type(trials) is not int or trials<3:raise Refused('At least three measured trials required')
    report={'status':'running','trials':trials,'warmup':[],'samples':[],'median_ms':{},
            'scope':'callable execution plus result fetch; setup excluded only if outside callable; no cost extrapolation'}
    def persist():
        if record is not None:write_json(record,report)
    def invoke(name,execute,phase,trial=None,expected=None):
        start=time.perf_counter_ns();sample={'side':name,'trial':trial}
        try:
            result=execute();sample['elapsed_ms']=(time.perf_counter_ns()-start)/1e6
            sample['correct']=None if expected is None else result_equal(expected,result,ordered=True)
            report[phase].append(sample);persist()
            if sample['correct'] is False:raise Refused('Result changed during measurement')
            return result
        except Exception as exc:
            sample.setdefault('elapsed_ms',(time.perf_counter_ns()-start)/1e6)
            sample['error_type']=type(exc).__name__
            if not any(item is sample for item in report[phase]):report[phase].append(sample)
            report['status']='failed';persist();raise
    persist()
    expected=invoke('source',source,'warmup')
    invoke('target',target,'warmup',expected=expected)
    for trial in range(trials):
        for name,execute in ([('source',source),('target',target)] if trial%2==0 else [('target',target),('source',source)]):
            invoke(name,execute,'samples',trial=trial,expected=expected)
    report['median_ms']={side:statistics.median(r['elapsed_ms'] for r in report['samples'] if r['side']==side) for side in ('source','target')}
    report['status']='passed';persist();return report
