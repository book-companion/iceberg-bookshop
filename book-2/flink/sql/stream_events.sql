-- Chapter 11: an unbounded append stream at 5 s checkpoints into a day-partitioned table.
-- Create the table from Spark first (Flink DDL cannot express days(ts)):
--   CREATE TABLE ice.bookshop.events (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP)
--   USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2', 'bookshop.class'='stream');
-- Then, inside the jobmanager:  ./bin/sql-client.sh -f /scripts/stream_events.sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '5s';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG ice WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://catalog:8181', 'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', 's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true', 'client.region' = 'us-east-1'
);

CREATE TEMPORARY TABLE gen (event_id BIGINT, customer_id INT, amount DECIMAL(10,2), ts TIMESTAMP(3)) WITH (
  'connector' = 'datagen', 'rows-per-second' = '200', 'fields.event_id.kind' = 'random',
  'fields.customer_id.min' = '1', 'fields.customer_id.max' = '500',
  'fields.amount.min' = '1', 'fields.amount.max' = '200', 'fields.ts.max-past' = '172800000'
);

INSERT INTO ice.bookshop.events SELECT event_id, customer_id, amount, CAST(ts AS TIMESTAMP(6)) FROM gen;
