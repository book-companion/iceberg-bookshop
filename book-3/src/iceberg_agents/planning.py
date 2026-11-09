"""Append-only local review decisions; accepting a plan grants no execution."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import uuid

from .contracts import Refused, canonical, digest, now


def review(work: Path, proposal: dict, *, reviewer: str, decision: str, reason_code: str) -> dict:
    required={'kind','table','base_snapshot_id','observation_sha256','action','eligible_for_operator_review','executes','excluded_actions'}
    if not required <= proposal.keys() or proposal['kind']!='proposal' or proposal['executes'] is not False:
        raise Refused('plan_shape','Review accepts a nonexecuting maintenance proposal')
    if not reviewer or decision not in ('accept','reject') or not reason_code or len(reason_code)>128:
        raise Refused('review_identity','Identify the reviewer, decision and a short reason code')
    if decision=='accept' and proposal['eligible_for_operator_review'] is not True:
        raise Refused('plan_blocked','Resolve the blocking conditions before accepting a plan for scheduling')
    work.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(work/'plan_reviews.sqlite')
    result={'review_id':uuid.uuid4().hex,'proposal_sha256':digest(proposal),'reviewer':reviewer,
            'decision':decision,'reason_code':reason_code,'observed_at':now(),'execution_authorized':False}
    try:
        db.execute('CREATE TABLE IF NOT EXISTS reviews(review_id TEXT PRIMARY KEY,proposal_sha256 TEXT NOT NULL,proposal_json TEXT NOT NULL,reviewer TEXT NOT NULL,decision TEXT NOT NULL,reason_code TEXT NOT NULL,observed_at TEXT NOT NULL)')
        with db:
            db.execute('INSERT INTO reviews VALUES(?,?,?,?,?,?,?)',(result['review_id'],result['proposal_sha256'],canonical(proposal),reviewer,decision,reason_code,result['observed_at']))
        return result
    finally:db.close()
