"""Executable single-process rehearsal. It does not fence external services."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .common import Refused, canonical, digest, now
from .fixture import validate_row


class Rehearsal:
    def __init__(self,path:Path):
        path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path)
        self.db.executescript('CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), writer TEXT, reader TEXT, target_writes INTEGER); CREATE TABLE IF NOT EXISTS data(side TEXT,key INTEGER,payload TEXT,PRIMARY KEY(side,key)); CREATE TABLE IF NOT EXISTS log(id INTEGER PRIMARY KEY,at TEXT,action TEXT,evidence TEXT);')

    def _log(self,action,evidence):
        self.db.execute('INSERT INTO log(at,action,evidence) VALUES (?,?,?)',(now(),action,canonical(evidence)))

    def seed(self,rows):
        if self.db.execute('SELECT 1 FROM state').fetchone():raise Refused('Rehearsal already initialized')
        with self.db:
            for side in ('source','target'):
                for row in rows:
                    item=validate_row(row)
                    self.db.execute('INSERT INTO data VALUES (?,?,?)',(side,item['order_id'],canonical(item)))
            self.db.execute("INSERT INTO state VALUES (1,'source','source',0)")
            self._log('seed',{'rows':len(rows)})

    def image(self,side):
        if side not in ('source','target'):raise Refused('Unknown side')
        return [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM data WHERE side=? ORDER BY key',(side,))]

    def state(self):
        row=self.db.execute('SELECT writer,reader,target_writes FROM state').fetchone()
        if not row:raise Refused('Uninitialized rehearsal')
        return dict(zip(('writer','reader','target_writes'),row))

    def freeze(self):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            previous=self.state()
            self.db.execute("UPDATE state SET writer='none' WHERE id=1")
            self._log('freeze',previous)

    def route(self,side):
        if side not in ('source','target'):raise Refused('Unknown route')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            state=self.state()
            if state['writer']!='none':raise Refused('Freeze the current writer before routing')
            if side=='source' and state['target_writes']:raise Refused('Target writes need reconciliation and explicit recovery')
            if digest(self.image('source'))!=digest(self.image('target')):raise Refused('Images disagree')
            self.db.execute('UPDATE state SET reader=? WHERE id=1',(side,))
            self._log('route',{'side':side})

    def enable_writer(self,side):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            state=self.state()
            if side not in ('source','target') or state['writer']!='none' or state['reader']!=side:raise Refused('Writer must follow the frozen, validated reader route')
            self.db.execute('UPDATE state SET writer=? WHERE id=1',(side,))
            self._log('enable_writer',{'side':side})

    def write(self,side,row):
        item=validate_row(row)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.state()['writer']!=side:raise Refused('This side is fenced in the rehearsal')
            self.db.execute('INSERT INTO data VALUES (?,?,?) ON CONFLICT(side,key) DO UPDATE SET payload=excluded.payload',(side,item['order_id'],canonical(item)))
            if side=='target':self.db.execute('UPDATE state SET target_writes=target_writes+1 WHERE id=1')
            self._log('write',{'side':side,'key':item['order_id']})

    def recover(self,*,approved_target_digest):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.state()['writer']!='none':raise Refused('Freeze writers before recovery')
            target=self.image('target')
            if digest(target)!=approved_target_digest:raise Refused('Recovery approval does not match target state')
            self.db.execute("DELETE FROM data WHERE side='source'")
            self.db.execute("INSERT INTO data SELECT 'source',key,payload FROM data WHERE side='target'")
            self.db.execute("UPDATE state SET reader='source',writer='none',target_writes=0 WHERE id=1")
            self._log('recovered_exact_image',{'target_digest':approved_target_digest,'rows':len(target)})
        return {'source_digest':digest(self.image('source')),'target_digest':digest(target),
                'scope':'single local SQLite transaction; production reverse mapping and distributed fencing need separate proof'}

    def close(self):self.db.close()
