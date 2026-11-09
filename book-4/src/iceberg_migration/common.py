from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class Refused(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.'+path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def fresh(work: Path):
    if work.exists() and any(work.iterdir()):
        raise Refused('Use a fresh work directory; existing evidence is never overwritten')
    work.mkdir(parents=True, exist_ok=True)


def read_json(path):
    return json.loads(Path(path).read_text())
