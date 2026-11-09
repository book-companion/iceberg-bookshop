"""Eight authored orders. Expected totals are assertions, not captured output."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .common import Refused, digest

FIELDS = ('order_id','tenant','customer_id','ordered_at','status','currency','gross_cents','discount_cents','refund_cents')
SCHEMA = {'order_id':'long','tenant':'string','customer_id':'long','ordered_at':'timestamptz',
          'status':'string','currency':'string','gross_cents':'long','discount_cents':'long','refund_cents':'long'}


def rows():
    values = [(41001,'north',101,1,'paid',10000,1000,0),(41002,'north',102,1,'paid',5000,0,1000),
              (41003,'north',103,1,'cancelled',2000,0,0),(41004,'south',201,1,'paid',8000,0,0),
              (41005,'south',202,2,'paid',12000,2000,0),(41006,'north',104,2,'paid',3500,500,0),
              (41007,'south',201,2,'pending',5000,0,0),(41008,'north',101,2,'paid',7000,0,2000)]
    return [dict(zip(FIELDS,(key,tenant,customer,f'2026-09-{day:02d}T12:00:00+00:00',status,'USD',gross,discount,refund)))
            for key,tenant,customer,day,status,gross,discount,refund in values]


def validate_row(row):
    if set(row)!=set(FIELDS):
        raise Refused('Column set does not match the declared mapping')
    for field in ('order_id','customer_id','gross_cents','discount_cents','refund_cents'):
        if type(row[field]) is not int or not 0 <= row[field] < 2**63:
            raise Refused('Integer cents and keys must be nonnegative signed-64-bit values')
    if row['tenant'] not in ('north','south') or row['status'] not in ('paid','pending','cancelled') or row['currency']!='USD':
        raise Refused('Unmapped business value')
    instant=datetime.fromisoformat(row['ordered_at'])
    if instant.tzinfo is None:
        raise Refused('A source timestamp needs an explicit offset')
    if row['discount_cents']+row['refund_cents']>row['gross_cents']:
        raise Refused('Discount plus refund exceeds gross')
    return {**row,'ordered_at':instant.astimezone(timezone.utc).isoformat()}


def revenue(data):
    return sum(r['gross_cents']-r['discount_cents']-r['refund_cents'] for r in data if r['status']=='paid')


def arrow(data):
    import pyarrow as pa
    fields=[pa.field(k,pa.timestamp('us',tz='UTC') if t=='timestamptz' else pa.int64() if t=='long' else pa.string(),nullable=False) for k,t in SCHEMA.items()]
    normalized=[validate_row(r) for r in data]
    return pa.Table.from_pylist([{**r,'ordered_at':datetime.fromisoformat(r['ordered_at'])} for r in normalized],schema=pa.schema(fields))


def json_rows(table):
    return [{**r,'ordered_at':r['ordered_at'].astimezone(timezone.utc).isoformat()} for r in table.to_pylist()]


def catalog(work: Path):
    from pyiceberg.catalog import load_catalog
    work=work.resolve();work.mkdir(parents=True,exist_ok=True)
    return load_catalog('migration',type='sql',uri='sqlite:///'+str(work/'catalog.sqlite'),warehouse=(work/'warehouse').as_uri())


def create_table(work: Path, data, name='migr_pilot.orders'):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField,LongType,StringType,TimestamptzType
    cat=catalog(work);namespace=name.split('.')[0]
    cat.create_namespace_if_not_exists(namespace)
    batch=arrow(data)
    types={'long':LongType(),'string':StringType(),'timestamptz':TimestamptzType()}
    schema=Schema(*(NestedField(i,key,types[typ],required=True) for i,(key,typ) in enumerate(SCHEMA.items(),1)))
    table=cat.create_table(name,schema=schema,properties={'format-version':'2'})
    table.append(batch)
    return table


def identity(table):
    snap=table.current_snapshot()
    return {'table_uuid':str(table.metadata.table_uuid),'snapshot_id':snap.snapshot_id if snap else None,
            'schema_id':table.schema().schema_id,'metadata_location':table.metadata_location}


def packet(data, boundary, *, schema=None):
    return {'boundary':boundary,'schema':schema or SCHEMA,'rows':data,'rows_sha256':digest(data)}


def target_packet(table):
    fields=table.schema().fields
    boundary={**identity(table),'physical_fields':[{'id':f.field_id,'name':f.name,'type':str(f.field_type),'required':f.required} for f in fields]}
    snapshot=boundary['snapshot_id']
    if snapshot is None:raise Refused('Capture requires a committed target snapshot')
    return packet(json_rows(table.scan(snapshot_id=snapshot).to_arrow()),boundary,schema={f.name:str(f.field_type) for f in fields})
