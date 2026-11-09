from __future__ import annotations

import argparse
from pathlib import Path

from .common import canonical,read_json,write_json,fresh


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--work',type=Path,default=Path('work'))
    sub=parser.add_subparsers(dest='command',required=True)
    sub.add_parser('pilot')
    p=sub.add_parser('inventory');p.add_argument('root',type=Path)
    p=sub.add_parser('reconcile');p.add_argument('source',type=Path);p.add_argument('target',type=Path);p.add_argument('--boundary',required=True)
    p=sub.add_parser('consumers');p.add_argument('source',type=Path);p.add_argument('target',type=Path)
    p=sub.add_parser('wave');p.add_argument('portfolio',type=Path)
    p=sub.add_parser('policy-parity');p.add_argument('expected',type=Path);p.add_argument('observed',type=Path)
    sub.add_parser('versions')
    args=parser.parse_args()
    if args.command=='pilot':
        from .probes import pilot
        fresh(args.work);result=pilot(args.work)
    elif args.command=='inventory':
        from .inventory import scan
        result=scan(args.root)
    elif args.command=='reconcile':
        from .reconcile import compare
        result=compare(read_json(args.source),read_json(args.target),logical_boundary=args.boundary)
    elif args.command=='consumers':
        from .consumers import compare_queries
        result=compare_queries(read_json(args.source)['rows'],read_json(args.target)['rows'])
    elif args.command=='wave':
        from .portfolio import wave
        result={'tables':wave(read_json(args.portfolio))}
    elif args.command=='policy-parity':
        from .policy import parity
        result=parity(read_json(args.expected),read_json(args.observed))
    else:
        import importlib.metadata,sys
        result={'python':sys.version,'packages':{}}
        for name in ('pyiceberg','pyarrow','duckdb','pyspark','psycopg','pytest'):
            try:result['packages'][name]=importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:result['packages'][name]=None
    print(canonical(result))
    if result.get('status')=='failed':raise SystemExit(2)


if __name__=='__main__':main()
