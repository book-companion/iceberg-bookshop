"""Migration handoff contract over an ordered, authored event stream.

This tests boundary arithmetic and replay, not a replacement CDC connector.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .common import Refused, canonical, digest
from .fixture import validate_row


class Handoff:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path)
        self.db.executescript('CREATE TABLE IF NOT EXISTS position(singleton INTEGER PRIMARY KEY CHECK(singleton=1), boundary INTEGER, applied INTEGER); CREATE TABLE IF NOT EXISTS rows(key INTEGER PRIMARY KEY, payload TEXT NOT NULL); CREATE TABLE IF NOT EXISTS events(position INTEGER PRIMARY KEY, digest TEXT NOT NULL);')

    def seed(self, data, position):
        if type(position) is not int or position<0:raise Refused('Invalid boundary')
        if self.db.execute('SELECT 1 FROM position').fetchone():raise Refused('Backfill already installed')
        normalized=[validate_row(row) for row in data]
        if len({r['order_id'] for r in normalized})!=len(normalized):raise Refused('Duplicate backfill key')
        with self.db:
            self.db.executemany('INSERT INTO rows VALUES (?,?)',[(r['order_id'],canonical(r)) for r in normalized])
            self.db.execute('INSERT INTO position VALUES (1,?,?)',(position,position))

    def apply(self, events, *, fail_before_commit=False):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            state=self.db.execute('SELECT boundary,applied FROM position').fetchone()
            if not state:raise Refused('Install a backfill before events')
            boundary,applied=state
            for event in events:
                pos=event.get('position')
                if type(pos) is not int or pos<0:raise Refused('Invalid event position')
                if pos<=boundary:continue
                previous=self.db.execute('SELECT digest FROM events WHERE position=?',(pos,)).fetchone()
                if previous:
                    if previous[0]!=digest(event):raise Refused('Conflicting reuse of event position')
                    continue
                if pos!=applied+1:raise Refused('Gap or out-of-order event in the declared single stream')
                if event.get('operation')=='upsert':
                    row=validate_row(event['row'])
                    self.db.execute('INSERT INTO rows VALUES (?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload',(row['order_id'],canonical(row)))
                elif event.get('operation')=='delete' and type(event.get('key')) is int:
                    self.db.execute('DELETE FROM rows WHERE key=?',(event['key'],))
                else:raise Refused('Unmapped event operation')
                self.db.execute('INSERT INTO events VALUES (?,?)',(pos,digest(event)))
                applied=pos
            self.db.execute('UPDATE position SET applied=? WHERE singleton=1',(applied,))
            if fail_before_commit:raise RuntimeError('Authored interruption before transaction commit')
        return self.image()

    def image(self):
        state=self.db.execute('SELECT boundary,applied FROM position').fetchone()
        return {'backfill_boundary':state[0],'applied_position':state[1],
                'rows':[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM rows ORDER BY key')]}

    def close(self):self.db.close()
