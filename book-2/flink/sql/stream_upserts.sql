-- Chapter 11: an upsert stream over a small key space. Every checkpoint adds one data
-- file, one position-delete file (keys upserted more than once within the checkpoint)
-- and one equality-delete file (keys that existed before it). Maintenance is the
-- data-file rewrite with delete-file-threshold=1, scheduled by checkpoint count.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '5s';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG ice WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://catalog:8181', 'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', 's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true', 'client.region' = 'us-east-1'
);
CREATE DATABASE IF NOT EXISTS ice.bookshop;
CREATE TABLE IF NOT EXISTS ice.bookshop.order_status (
  order_id BIGINT, status STRING, amount DECIMAL(10,2), ts TIMESTAMP(6), PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TEMPORARY TABLE g (order_id BIGINT, status STRING, amount DECIMAL(10,2), ts TIMESTAMP(3)) WITH (
  'connector' = 'datagen', 'rows-per-second' = '500',
  'fields.order_id.min' = '1', 'fields.order_id.max' = '1000', 'fields.status.length' = '6',
  'fields.amount.min' = '1', 'fields.amount.max' = '200'
);

INSERT INTO ice.bookshop.order_status SELECT order_id, status, amount, CAST(ts AS TIMESTAMP(6)) FROM g;
