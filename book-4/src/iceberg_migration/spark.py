"""Fresh-process Spark profiles. Only generated migr_* identifiers are mutated."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from .common import Refused
from .fixture import FIELDS, rows

ICEBERG='org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0'
DELTA='io.delta:delta-spark_4.1_2.13:4.1.0'
HUDI='org.apache.hudi:hudi-spark4.1-bundle_2.13:1.2.0'


def session(work:Path,*,source=None,rest=False,hive=False):
    import time
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    from pyspark.sql import SparkSession
    packages=[ICEBERG]
    extensions=['org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions']
    config={'spark.ui.enabled':'false','spark.sql.session.timeZone':'UTC','spark.sql.catalog.ice':'org.apache.iceberg.spark.SparkCatalog','spark.sql.catalog.ice.cache-enabled':'false'}
    if rest:
        required=('MIGR_REST_URI','MIGR_WAREHOUSE','MIGR_CREDENTIAL','MIGR_S3_ENDPOINT')
        if any(not os.environ.get(k) for k in required):raise Refused('Configure the scoped migration REST environment')
        packages.append('org.apache.iceberg:iceberg-aws-bundle:1.11.0')
        config.update({'spark.sql.catalog.ice.type':'rest','spark.sql.catalog.ice.uri':os.environ['MIGR_REST_URI'],
          'spark.sql.catalog.ice.warehouse':os.environ['MIGR_WAREHOUSE'],'spark.sql.catalog.ice.credential':os.environ['MIGR_CREDENTIAL'],
          'spark.sql.catalog.ice.scope':os.environ.get('MIGR_SCOPE','PRINCIPAL_ROLE:ALL'),
          'spark.sql.catalog.ice.io-impl':'org.apache.iceberg.aws.s3.S3FileIO',
          'spark.sql.catalog.ice.s3.endpoint':os.environ['MIGR_S3_ENDPOINT'],'spark.sql.catalog.ice.s3.path-style-access':'true'})
    else:config.update({'spark.sql.catalog.ice.type':'hadoop','spark.sql.catalog.ice.warehouse':(work/'iceberg').resolve().as_uri()})
    if source=='delta':
        packages.append(DELTA);extensions.append('io.delta.sql.DeltaSparkSessionExtension')
        config['spark.sql.catalog.spark_catalog']='org.apache.spark.sql.delta.catalog.DeltaCatalog'
    if source=='hudi':
        packages.append(HUDI);extensions.append('org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
        config.update({'spark.serializer':'org.apache.spark.serializer.KryoSerializer','spark.kryo.registrator':'org.apache.spark.HoodieSparkKryoRegistrar','spark.sql.catalog.spark_catalog':'org.apache.spark.sql.hudi.catalog.HoodieCatalog'})
    if hive:
        if not os.environ.get('MIGR_HMS_URI'):raise Refused('Set MIGR_HMS_URI to the owned migration metastore')
        config.update({'spark.sql.catalog.spark_catalog':'org.apache.iceberg.spark.SparkSessionCatalog','spark.sql.catalog.spark_catalog.type':'hive',
                       'spark.hadoop.hive.metastore.uris':os.environ['MIGR_HMS_URI'],
                       'spark.sql.warehouse.dir':(work/'hive-warehouse').resolve().as_uri()})
    builder=SparkSession.builder.master('local[2]').appName('book4-migration')
    config.update({'spark.jars.packages':','.join(packages),'spark.sql.extensions':','.join(extensions)})
    for key,value in config.items():builder=builder.config(key,value)
    if hive:builder=builder.enableHiveSupport()
    spark=builder.getOrCreate();spark.sparkContext.setLogLevel('WARN')
    if spark.version!='4.1.3':spark.stop();raise Refused('Expected Spark 4.1.3; record and investigate a different runtime')
    return spark


def frame(spark,data=None):
    records=[{**row,'ordered_at':datetime.fromisoformat(row['ordered_at'])} for row in (rows() if data is None else data)]
    return spark.createDataFrame(records,schema='order_id long, tenant string, customer_id long, ordered_at timestamp, status string, currency string, gross_cents long, discount_cents long, refund_cents long')


def collect(dataframe):
    records=[]
    for row in dataframe.select(*FIELDS).collect():
        value=row.asDict();stamp=value['ordered_at']
        # session() sets the process timezone to UTC before creating Spark.
        if stamp.tzinfo is None:stamp=stamp.replace(tzinfo=timezone.utc)
        value['ordered_at']=stamp.astimezone(timezone.utc).isoformat();records.append(value)
    return sorted(records,key=lambda row:row['order_id'])
