-- Debezium -> Kafka -> Flink -> Iceberg. The bookshop's `orders`
-- table (PostgreSQL 18, wal_level=logical) arrives on topic
-- bookshop.public.orders as Debezium envelopes; Flink's debezium-json format
-- turns them into a changelog, and an UPSERT-mode Iceberg table (primary key
-- order_id) absorbs inserts, updates and deletes as data + equality deletes.
-- Streaming, 10 s checkpoints; cancelled from the host when converged.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';

CREATE CATALOG ice WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://catalog:8181',
  'warehouse' = 's3://warehouse/', 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333', 's3.path-style-access' = 'true', 'client.region' = 'us-east-1'
);
CREATE DATABASE IF NOT EXISTS ice.stage4;
DROP TABLE IF EXISTS ice.stage4.orders_cdc;
CREATE TABLE ice.stage4.orders_cdc (
  order_id INT, customer_id INT, order_date INT, status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

-- order_date arrives as Debezium's io.debezium.time.Date: an INT of days
-- since the epoch, so it is declared INT here and converted downstream.
CREATE TEMPORARY TABLE orders_src (
  order_id INT, customer_id INT, order_date INT, status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka', 'topic' = 'bookshop.public.orders',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'flink-stage4c',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json', 'debezium-json.schema-include' = 'true'
);
INSERT INTO ice.stage4.orders_cdc SELECT * FROM orders_src;
