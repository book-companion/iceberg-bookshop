-- a STREAMING job with a 10 s checkpoint interval writing a bounded
-- datagen source into Iceberg. Every checkpoint is one Iceberg commit, so the
-- snapshot count afterwards is the checkpoint count, not the row count —
-- the "commit rate versus metadata growth" coupling of chapter 11.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG ice WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://catalog:8181',
  'warehouse' = 's3://warehouse/', 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333', 's3.path-style-access' = 'true', 'client.region' = 'us-east-1'
);
CREATE DATABASE IF NOT EXISTS ice.stage4;
DROP TABLE IF EXISTS ice.stage4.events;
CREATE TABLE ice.stage4.events (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP(3))
  WITH ('format-version' = '2');

-- 60,000 rows at 1,000 rows/s: about a minute, so about six checkpoints.
CREATE TEMPORARY TABLE gen (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP(3)) WITH (
  'connector' = 'datagen', 'number-of-rows' = '60000', 'rows-per-second' = '1000',
  'fields.event_id.kind' = 'sequence', 'fields.event_id.start' = '1', 'fields.event_id.end' = '60000',
  'fields.customer_id.min' = '1', 'fields.customer_id.max' = '500',
  'fields.amount.min' = '1', 'fields.amount.max' = '200'
);
INSERT INTO ice.stage4.events SELECT * FROM gen;
