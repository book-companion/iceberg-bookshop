#!/usr/bin/env python3
"""Read Book 2's existing health pack using Book 2's interpreter and catalog.

No --store option: this bridge does not write the shared ops table. It retains
raw nulls rather than parsing the pack's human-oriented formatted output.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('namespaces',nargs='+')
    parser.add_argument('--output',type=Path,default=Path('work/health-pack'))
    args=parser.parse_args()
    if any(not re.fullmatch(r'[a-z][a-z0-9_]*',name) for name in args.namespaces):
        parser.error('Use simple existing namespace names')
    root=Path(__file__).resolve().parents[1]
    path=root/'book-2'/'health'/'table_health.py'
    spec=importlib.util.spec_from_file_location('book2_health_pack',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    spark=module.spark('book3-read-existing-health-pack')
    rows=[]
    try:
        sql=lambda query:spark.sql(query).collect()
        storage=module.s3client()
        for namespace in args.namespaces:
            for table in sql(f'SHOW TABLES IN ice.{namespace}'):
                if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*',table[1]):
                    raise ValueError('The bridge accepts simple table identifiers only')
                row=module.health(sql,storage,namespace,table[1])
                if row.get('files') is None or row.get('partitions') is None:
                    row['excess_files']=None
                if row.get('delete_rows') is None or row.get('files') is None:
                    row['deletes_per_file']=None
                rows.append(row)
    finally:spark.stop()
    if not rows:raise RuntimeError('No tables were observed; seed the Book 2 control fleet first')
    stamp=datetime.now(timezone.utc).isoformat()
    sys.path.insert(0,str(root/'book-3'/'src'))
    from iceberg_agents.health_cases import pack_findings
    observations=[]
    for row in rows:
        for signal,value in row.items():
            if signal not in ('run_at','catalog','tbl'):
                observations.append({'id':row['tbl']+':'+signal,'observed_at':stamp,'signal':signal,'value':value,'table':row['tbl']})
        observations.append({'id':row['tbl']+':rules','observed_at':stamp,'signal':'deterministic_findings','value':pack_findings(row),'table':row['tbl']})
    args.output.mkdir(parents=True,exist_ok=False)
    (args.output/'raw.json').write_text(json.dumps({'schema':'book2-health-pack-1','captured_at':stamp,'rows':rows},default=str,indent=2)+'\n')
    (args.output/'observations.json').write_text(json.dumps(observations,default=str,indent=2)+'\n')
    print(json.dumps({'tables':len(rows),'observations':len(observations),'output':str(args.output)}))


if __name__=='__main__':main()
