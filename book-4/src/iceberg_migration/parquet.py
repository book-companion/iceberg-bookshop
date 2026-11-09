"""File-level quarantine with hashes; no silent row dropping or source deletion."""
from __future__ import annotations

import hashlib
from pathlib import Path

from .common import Refused, write_json
from .fixture import arrow, json_rows, rows, validate_row


def seed_zone(root: Path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if root.exists():raise Refused('Source zone already exists')
    root.mkdir(parents=True)
    pq.write_table(arrow(rows()[:4]),root/'part-a.parquet',compression='snappy')
    pq.write_table(arrow(rows()[4:]),root/'part-b.parquet',compression='snappy')
    (root/'broken.parquet').write_bytes(b'authored corrupt fixture, not parquet')
    bad={**rows()[0],'order_id':41999,'refund_cents':'unknown'}
    pq.write_table(pa.Table.from_pylist([bad]),root/'unmapped.parquet')


def normalize(root: Path, output: Path):
    import pyarrow.parquet as pq
    if output.exists():raise Refused('Normalization output already exists')
    files=sorted(root.rglob('*.parquet'))
    if not files:raise Refused('No source files')
    accepted=[];report=[];keys=set()
    for path in files:
        if path.is_symlink():raise Refused('Source symlinks require explicit ownership review')
        content=path.read_bytes();entry={'path':str(path.relative_to(root)),'sha256':hashlib.sha256(content).hexdigest()}
        try:
            table=pq.read_table(path)
            candidates=[]
            for row in table.to_pylist():
                value=row.get('ordered_at')
                if hasattr(value,'isoformat'):row['ordered_at']=value.isoformat()
                candidates.append(validate_row(row))
            newkeys=[r['order_id'] for r in candidates]
            if len(set(newkeys))!=len(newkeys) or set(newkeys)&keys:raise Refused('Duplicate key across accepted source files')
            if hashlib.sha256(path.read_bytes()).hexdigest()!=entry['sha256']:raise Refused('Source file changed during capture')
            keys.update(newkeys);accepted.extend(candidates);entry.update(status='accepted',rows=len(candidates))
        except Exception as exc:
            entry.update(status='quarantined',reason_type=type(exc).__name__,
                         reason=str(exc) if isinstance(exc,Refused) else 'Unreadable Parquet data or incompatible physical schema')
        report.append(entry)
    if not accepted:raise Refused('No accepted data; an empty target would conceal quarantine')
    output.mkdir(parents=True)
    pq.write_table(arrow(accepted),output/'accepted.parquet')
    result={'files':report,'accepted_rows':len(accepted),'quarantined_files':sum(r['status']=='quarantined' for r in report),
            'release_eligible':all(r['status']=='accepted' for r in report),'source_files_deleted':False}
    write_json(output/'report.json',result)
    return accepted,result
