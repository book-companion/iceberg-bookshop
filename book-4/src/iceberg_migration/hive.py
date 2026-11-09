"""Owned HMS fixtures: shared-file adoption, migration and duplicate registration."""
from __future__ import annotations

import uuid
import os
from pathlib import Path
from urllib.parse import unquote,urlparse

from .common import Refused,write_json
from .fixture import rows
from .spark import session,frame,collect


def file_identity(value):
    uri=urlparse(value)
    return Path(unquote(uri.path)).resolve().as_uri() if uri.scheme in ('','file') else value


def exercise(work:Path):
    if any(c in str(work) for c in ("'",'`')):raise Refused('Use a work path without SQL quote characters')
    shared=os.environ.get('MIGR_HIVE_WORKSPACE')
    if not shared or not Path(shared).is_absolute() or not work.resolve().is_relative_to(Path(shared).resolve()):raise Refused('Use a work directory under the absolute MIGR_HIVE_WORKSPACE mounted at the identical path in the metastore')
    spark=session(work,hive=True);namespace='migr_'+uuid.uuid4().hex[:12]
    source=f'spark_catalog.{namespace}.source';snapshot=f'spark_catalog.{namespace}.snapshot'
    migration=f'spark_catalog.{namespace}.convert';registered=f'ice.{namespace}.registered'
    try:
        spark.sql(f'CREATE NAMESPACE spark_catalog.{namespace}')
        source_path=(work/'hive-source').resolve()
        frame(spark).write.format('parquet').option('compression','gzip').option('path',str(source_path)).mode('errorifexists').saveAsTable(source)
        assert collect(spark.table(source))==rows()
        before={file_identity(p) for p in spark.table(source).inputFiles()}
        spark.sql(f"CALL spark_catalog.system.snapshot(source_table => '{namespace}.source', table => '{namespace}.snapshot')").collect()
        assert collect(spark.table(snapshot))==rows()
        shared={file_identity(r[0]) for r in spark.sql(f'SELECT file_path FROM {snapshot}.files').collect()}
        assert shared==before,'Snapshot did not expose the expected shared source files'
        victim=Path(unquote(urlparse(sorted(shared)[0]).path)).resolve()
        if not victim.is_relative_to(source_path) or not victim.is_file():raise Refused('Cleanup hazard must stay inside this owned fixture')
        held=victim.with_suffix(victim.suffix+'.held')
        failure=None
        victim.rename(held)
        try:
            spark.catalog.clearCache()
            try:collect(spark.table(snapshot))
            except Exception as exc:failure=type(exc).__name__
            assert failure is not None,'A full data read unexpectedly survived the missing owned source file'
        finally:held.rename(victim)
        spark.catalog.clearCache();assert collect(spark.table(snapshot))==rows()
        converted_path=(work/'hive-convert').resolve()
        frame(spark).write.format('parquet').option('compression','gzip').option('path',str(converted_path)).mode('errorifexists').saveAsTable(migration)
        spark.sql(f"CALL spark_catalog.system.migrate(table => '{namespace}.convert')").collect()
        assert collect(spark.table(migration))==rows()
        properties={r[0]:r[1] for r in spark.sql(f'SHOW TBLPROPERTIES {migration}').collect()}
        import pyarrow.parquet as pq
        codecs_before=sorted({pq.read_metadata(p).row_group(0).column(0).compression for p in converted_path.rglob('*.parquet')})
        spark.sql(f"ALTER TABLE {migration} SET TBLPROPERTIES ('write.parquet.compression-codec'='zstd')")
        extra={**rows()[0],'order_id':41999}
        frame(spark,[extra]).writeTo(migration).append()
        codecs_after=sorted({pq.read_metadata(p).row_group(0).column(0).compression for p in converted_path.rglob('*.parquet')})
        assert 'GZIP' in codecs_before and 'GZIP' in codecs_after and 'ZSTD' in codecs_after
        spark.sql(f'CREATE NAMESPACE ice.{namespace}')
        frame(spark,[]).writeTo(registered).using('iceberg').create()
        source_identifier=f'parquet.`{source_path}`'
        call=f"CALL ice.system.add_files(table => '{namespace}.registered', source_table => '{source_identifier}'"
        spark.sql(call+')').collect()
        assert len(collect(spark.table(registered)))==8
        duplicate_refused=False
        try:spark.sql(call+')').collect()
        except Exception:duplicate_refused=True
        assert duplicate_refused,'Expected the default duplicate check to reject repeat registration'
        spark.sql(call+', check_duplicate_files => false)').collect()
        duplicated=collect(spark.table(registered))
        assert len(duplicated)==16,'Unsafe duplicate-registration experiment did not double fixture rows'
        result={'status':'passed','namespace':namespace,'source':source,'snapshot':snapshot,'shared_file_count':len(shared),
                'missing_file_error_type':failure,'restored_read':True,'migrated_table':migration,
                'properties_after_migrate':properties,'old_file_codecs':codecs_before,'after_explicit_zstd_append':codecs_after,
                'duplicate_default_refused':duplicate_refused,'unsafe_registered_rows':len(duplicated),
                'scope':'disposable owned files only; source file temporarily moved and restored; no shared cleanup'}
        write_json(work/'hive.json',result);return result
    finally:spark.stop()
