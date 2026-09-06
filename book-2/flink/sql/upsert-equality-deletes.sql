-- Flink 2.1 writing to Iceberg through the REST fixture on S3, in
-- UPSERT mode on a keyed table. A second INSERT for existing keys must produce
-- EQUALITY delete files — the artifact Book 1 chapter 17 could only describe.
-- Run in batch mode: sql-client.sh -f /scripts/upsert-equality-deletes.sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG ice WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://catalog:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  'client.region' = 'us-east-1'
);
CREATE DATABASE IF NOT EXISTS ice.stage4;
DROP TABLE IF EXISTS ice.stage4.orders_upsert;
CREATE TABLE ice.stage4.orders_upsert (
  order_id BIGINT, country STRING, amount DECIMAL(10,2), status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

INSERT INTO ice.stage4.orders_upsert VALUES
  (1, 'DE', 10.00, 'placed'), (2, 'GB', 20.00, 'placed'), (3, 'US', 30.00, 'placed'),
  (4, 'DE', 40.00, 'placed'), (5, 'GB', 50.00, 'placed');

-- the same keys again: an upsert writer expresses "replace row 1..3" as
-- equality deletes + new data, never as a rewrite of the old file
INSERT INTO ice.stage4.orders_upsert VALUES
  (1, 'DE', 10.00, 'shipped'), (2, 'GB', 20.00, 'shipped'), (3, 'US', 30.00, 'cancelled');

SELECT status, count(*) AS n FROM ice.stage4.orders_upsert GROUP BY status;
