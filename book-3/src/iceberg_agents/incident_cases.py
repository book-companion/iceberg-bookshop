"""Authored incident replays based on Book 2's runbook categories.

These are inputs and review labels, not captured outages or completed recoveries.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def cases() -> list[dict]:
    specs=[
        ('stray_metadata',[('unreferenced_metadata_objects',1),('table_load','succeeds'),('column_read','succeeds')],
         'Compare the object with current and retained metadata references and active writer activity before proposing cleanup.'),
        ('missing_data',[('metadata_count',8),('column_read','missing object'),('referenced_object_head','not found')],
         'Check object availability and the snapshots that reference it; a tag does not recreate a missing object.'),
        ('accidental_overwrite',[('previous_row_count',8),('current_row_count',1),('latest_operation','overwrite'),('writer_state','unknown')],
         'Identify the intended overwrite and prior retained state; establish writer ownership before recovery.'),
        ('dropped_column',[('requested_column','refund_cents'),('column_resolution','missing'),('readded_same_name','new field id')],
         'Compare field ids and historical schemas; reusing the name does not restore the original field identity.'),
        ('stale_pointer',[('catalog_metadata_version',3),('observed_object_versions',[3,4,5]),('commit_provenance','unknown')],
         'Determine which metadata files were actually committed; the highest filename alone is not authority.'),
        ('corrupt_metadata',[('catalog_load','parse error'),('second_client_load','parse error'),('previous_metadata_object','present')],
         'Preserve the failing object and inspect its integrity and committed predecessor before proposing restoration.'),
        ('expired_recovery',[('requested_snapshot','absent from metadata'),('current_read','succeeds'),('retained_reference','none recorded')],
         'Inspect retained references, backups and source replay options; do not promise rollback to an absent snapshot.'),
        ('cleanup_race',[('writer_duration_minutes',90),('cleanup_age_minutes',30),('writer_state','active'),('unreferenced_files','observed')],
         'Compare active write windows with cleanup eligibility; unreferenced files may belong to an unfinished write.'),
    ]
    return [{'id':key,'observations':[{'id':key+':'+signal,'observed_at':'authored-replay','signal':signal,'value':value} for signal,value in signals],
             'expected':[check],'source':'book-2/runbooks/README.md categories; newly authored inputs','recovery_executed':False}
            for key,signals,check in specs]


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    entries=cases()
    (args.output/'cases.json').write_text(json.dumps(entries,indent=2)+'\n')
    for case in entries:
        (args.output/(case['id']+'.observations.json')).write_text(json.dumps(case['observations'],indent=2)+'\n')
    print(json.dumps({'authored_cases':len(entries),'output':str(args.output),'recovery_executed':False}))


if __name__=='__main__':main()
