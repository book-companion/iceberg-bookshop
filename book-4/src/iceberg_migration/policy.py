"""Valid positive controls precede catalog-denial interpretation."""
from __future__ import annotations

import os

from .common import Refused


def catalog(credential):
    from pyiceberg.catalog import load_catalog
    required=('MIGR_REST_URI','MIGR_WAREHOUSE','MIGR_S3_ENDPOINT')
    if any(not os.environ.get(k) for k in required):raise Refused('Missing migration REST configuration')
    return load_catalog('migration',type='rest',uri=os.environ['MIGR_REST_URI'],warehouse=os.environ['MIGR_WAREHOUSE'],
                        credential=credential,scope=os.environ.get('MIGR_SCOPE','PRINCIPAL_ROLE:ALL'),
                        **{'s3.endpoint':os.environ['MIGR_S3_ENDPOINT']})


def controls():
    from pyiceberg.exceptions import ForbiddenError
    required=('MIGR_CREDENTIAL','MIGR_RESTRICTED_CREDENTIAL','MIGR_ALLOWED_CONTROL','MIGR_FORBIDDEN_CONTROL')
    if any(not os.environ.get(k) for k in required):raise Refused('Provision operator and restricted principal plus two existing controls')
    admin=catalog(os.environ['MIGR_CREDENTIAL']);reader=catalog(os.environ['MIGR_RESTRICTED_CREDENTIAL'])
    allowed=os.environ['MIGR_ALLOWED_CONTROL'];forbidden=os.environ['MIGR_FORBIDDEN_CONTROL']
    admin.load_table(allowed);admin.load_table(forbidden)
    table=reader.load_table(allowed)
    if table.scan(limit=1).to_arrow().num_rows!=1:raise Refused('Allowed control must contain a readable row')
    try:reader.load_table(forbidden)
    except ForbiddenError:denied=True
    else:raise AssertionError('Restricted identity loaded the forbidden control')
    return {'status':'passed','allowed':allowed,'forbidden':forbidden,'denied':denied,
            'observations':[{'case_id':'analyst-read','identity':'restricted_reader','path':'PyIceberg REST','outcome':'allowed'},
                            {'case_id':'outside-scope','identity':'restricted_reader','path':'PyIceberg REST forbidden control','outcome':'denied','error_category':'authorization'}],
            'scope':'PyIceberg REST table-load denial and allowed data read; row/column policy and other engine paths need their own tests'}


def parity(expected,observed):
    required={row['case_id']:row for row in expected}
    actual={row['case_id']:row for row in observed}
    if len(required)!=len(expected) or len(actual)!=len(observed):raise Refused('Duplicate policy case')
    failures=[]
    if set(required)!=set(actual):failures.append('case_coverage')
    for key,want in required.items():
        got=actual.get(key,{})
        if got.get('identity')!=want['identity'] or got.get('path')!=want['path'] or got.get('outcome')!=want['outcome']:
            failures.append(key)
        if want['outcome']=='denied' and got.get('error_category')!='authorization':failures.append(key+':wrong_denial')
    return {'status':'passed' if not failures else 'failed','failures':failures,
            'scope':'comparison of recorded observations; collector must authenticate their origin'}
