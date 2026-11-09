"""Invoke the existing Book 2 convergence test; never rebuild its pipeline."""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from .common import Refused


def existing_convergence(work):
    root=Path(__file__).resolve().parents[3]
    python=root/'.venv/bin/python';script=root/'book-2/cdc/convergence_test.py'
    table=os.environ.get('MIGR_CDC_TABLE','ice.bookshop.orders_cdc')
    if not re.fullmatch(r'ice\.(?:bookshop|migr_[a-z0-9_]+)\.[a-z][a-z0-9_]*',table):raise Refused('Invalid existing CDC table identifier')
    if not python.is_file() or not script.is_file():raise Refused('Book 2 environment and convergence script are prerequisites')
    result=subprocess.run([str(python),str(script),table,'--timeout','300'],cwd=root/'book-2',capture_output=True,text=True,timeout=420)
    (work/'book2-cdc.stdout.txt').write_text(result.stdout);(work/'book2-cdc.stderr.txt').write_text(result.stderr)
    if result.returncode:raise Refused('Existing Book 2 convergence check failed; keep its actual output')
    return {'status':'passed','table':table,'scope':'Book 2 count/status/key-sum convergence under its source observation; not exact migration boundary proof',
            'next_evidence':'freeze/capture aligned source and target images, then run exact keyed reconciliation and migration boundary cases'}
