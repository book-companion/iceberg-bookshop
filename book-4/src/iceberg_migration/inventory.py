from __future__ import annotations

import hashlib
from pathlib import Path

from .common import Refused, now


def scan(root: Path):
    import pyarrow.parquet as pq
    if not root.is_dir():raise Refused('Source directory does not exist')
    files=[]
    for path in sorted(root.rglob('*.parquet')):
        if path.is_symlink():raise Refused('Resolve source ownership before following symlinks')
        item={'path':str(path.relative_to(root)),'bytes':path.stat().st_size,
              'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'owner':None,'update_contract':None}
        try:
            metadata=pq.read_metadata(path)
            item.update(status='readable',rows=metadata.num_rows,schema=str(metadata.schema.to_arrow_schema()))
        except Exception as exc:
            item.update(status='quarantine',reason='unreadable_footer',error_type=type(exc).__name__)
        files.append(item)
    if not files:raise Refused('An empty file inventory is not a discovered estate')
    return {'captured_at':now(),'source_root':str(root.resolve()),'files':files,
            'consistency':'local directory enumeration; writers must be frozen or files immutable',
            'ownership_confirmed':False}


def readiness(item):
    required=('owner','consumer_owner','source_boundary','recovery_owner')
    missing=[key for key in required if not item.get(key)]
    if item.get('unsupported_features'):missing.append('unsupported_features')
    return {'eligible':not missing,'missing':missing,
            'priority_score':int(item.get('benefit',0))-int(item.get('complexity',0)),
            'score_is_author_judgment':True}
