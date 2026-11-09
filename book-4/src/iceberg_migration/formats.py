"""Feasibility first: actual source mutations and exact current-state copy."""
from __future__ import annotations

import hashlib
import os
import subprocess
import uuid
from pathlib import Path

from .common import Refused, digest, write_json,read_json
from .fixture import rows, packet
from .reconcile import compare
from .spark import session,frame,collect,DELTA,HUDI


def feasibility(work:Path,source:str):
    if source not in ('delta','hudi'):raise Refused('Choose delta or hudi')
    if any(c in str(work) for c in ("'",'`')):raise Refused('Use a work path without SQL quote characters')
    spark=session(work,source=source);path=(work/source).resolve();ns='migr_'+uuid.uuid4().hex[:12]
    try:
        expected=rows();initial=frame(spark)
        if source=='delta':
            initial.write.format('delta').mode('errorifexists').save(str(path))
            spark.sql(f"UPDATE delta.`{path}` SET refund_cents=100 WHERE order_id=41001")
            spark.sql(f"DELETE FROM delta.`{path}` WHERE order_id=41002")
            prior=collect(spark.read.format('delta').option('versionAsOf',0).load(str(path)))
            assert prior==expected
            actual=collect(spark.read.format('delta').load(str(path)))
            history={'initial_version':0,'initial_rows':len(prior),'preserved_in_target':False}
            detail=spark.sql(f'DESCRIBE DETAIL delta.`{path}`').first().asDict()
            features={key:detail.get(key) for key in ('minReaderVersion','minWriterVersion','tableFeatures')}
        else:
            from pyspark.sql.functions import lit
            options={'hoodie.table.name':ns,'hoodie.datasource.write.recordkey.field':'order_id','hoodie.datasource.write.precombine.field':'_migration_seq',
                     'hoodie.datasource.write.table.type':'COPY_ON_WRITE','hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
                     'hoodie.datasource.write.operation':'upsert'}
            initial.withColumn('_migration_seq',lit(1).cast('long')).write.format('hudi').options(**options).mode('errorifexists').save(str(path))
            changed={**expected[0],'refund_cents':100}
            frame(spark,[changed]).withColumn('_migration_seq',lit(2).cast('long')).write.format('hudi').options(**options).mode('append').save(str(path))
            deletion={**options,'hoodie.datasource.write.operation':'delete'}
            frame(spark,[expected[1]]).withColumn('_migration_seq',lit(3).cast('long')).write.format('hudi').options(**deletion).mode('append').save(str(path))
            actual=collect(spark.read.format('hudi').load(str(path)))
            history={'source_timeline_present':(path/'.hoodie').exists(),'preserved_in_target':False}
            properties={}
            for line in (path/'.hoodie/hoodie.properties').read_text().splitlines():
                if '=' in line and not line.startswith('#'):
                    key,value=line.split('=',1);properties[key.strip()]=value.strip()
            features={key:properties.get(key) for key in ('hoodie.table.type','hoodie.table.version','hoodie.table.recordkey.fields','hoodie.table.precombine.field')}
        expected=[{**r,'refund_cents':100} if r['order_id']==41001 else r for r in expected if r['order_id']!=41002]
        assert actual==expected,'Source mutation/read semantics did not match the authored fixture'
        spark.sql(f'CREATE NAMESPACE ice.{ns}')
        frame(spark,actual).writeTo(f'ice.{ns}.orders').using('iceberg').tableProperty('format-version','2').create()
        target=collect(spark.table(f'ice.{ns}.orders'))
        source_packet=packet(actual,{'format':source,'path':str(path),'rows_digest':digest(actual),'source_writers':'probe only'})
        target_packet=packet(target,{'table':f'ice.{ns}.orders','catalog':'isolated HadoopCatalog'})
        report=compare(source_packet,target_packet,logical_boundary='source quiescent after update and delete')
        assert report['status']=='passed'
        result={'status':'passed','source':source,'coordinate':DELTA if source=='delta' else HUDI,'source_path':str(path),
                'target_table':f'ice.{ns}.orders','history':history,'source_features':features,'reconciliation':report,
                'scope':'current-state rewrite; no CDC, metadata translation, deletion-vector or MOR qualification'}
        write_json(work/'source.json',source_packet);write_json(work/'target.json',target_packet);write_json(work/'feasibility.json',result)
        return result
    finally:spark.stop()


def xtable(work:Path,*,source_path:Path,source_format:str,expected):
    if source_format not in ('DELTA','HUDI'):raise Refused('XTable probe source must be DELTA or HUDI')
    jar=Path(os.environ.get('MIGR_XTABLE_JAR',''))
    checksum=os.environ.get('MIGR_XTABLE_SHA256')
    if not jar.is_file() or not checksum:raise Refused('Provide an operator-selected XTable bundled jar and its independently recorded SHA256')
    actual=hashlib.sha256(jar.read_bytes()).hexdigest()
    if actual!=checksum:raise Refused('XTable artifact digest mismatch')
    source_path=source_path.resolve()
    if not source_path.is_dir():raise Refused('Source format probe must create a disposable source first')
    marker=source_path.parent/'feasibility.json'
    if not marker.is_file():raise Refused('Only a disposable source created by the format probe may receive translated metadata')
    evidence=read_json(marker)
    if evidence.get('status')!='passed' or evidence.get('source_path')!=str(source_path) or evidence.get('source')!=source_format.lower():raise Refused('Source marker does not match the requested XTable input')
    if expected.get('boundary',{}).get('path')!=str(source_path):raise Refused('Expected packet belongs to another source path')
    # JSON is valid YAML; no interpolation of paths into YAML or shell syntax.
    config={'sourceFormat':source_format,'targetFormats':['ICEBERG'],'datasets':[{'tableBasePath':source_path.as_uri(),'tableName':'migr_xtable'}]}
    write_json(work/'xtable-config.yaml',config)
    completed=subprocess.run(['java','-jar',str(jar),'--datasetConfig',str(work/'xtable-config.yaml')],capture_output=True,text=True,timeout=300)
    (work/'xtable.stdout.txt').write_text(completed.stdout);(work/'xtable.stderr.txt').write_text(completed.stderr)
    if completed.returncode:raise Refused('XTable sync failed; retain exact jar, config and output')
    spark=session(work)
    try:
        actual_rows=collect(spark.read.format('iceberg').load(str(source_path)))
        source_packet=expected
        destination=packet(actual_rows,{'path':str(source_path),'format':'Iceberg translated metadata','jar_sha256':actual})
        report=compare(source_packet,destination,logical_boundary='quiescent disposable source at supplied captured image')
        if report['status']!='passed':raise Refused('Translated view differs from captured logical source')
        result={'status':'passed','jar_sha256':actual,'reconciliation':report,'ownership':'shared files; source retention still matters'}
        write_json(work/'xtable.json',result);return result
    finally:spark.stop()
