-- Chapter 12: the curated table and its row lineage. The landing table (Flink upsert,
-- v2, equality deletes) cannot serve its own changes: a changelog scan refuses delete
-- files and an incremental append read returns zero rows silently. Consumers read this
-- table instead, by sequence-number watermark.

CREATE TABLE IF NOT EXISTS ice.bookshop.orders_curated (
  order_id INT, customer_id INT, order_date DATE, status STRING
) USING iceberg TBLPROPERTIES ('format-version' = '3', 'bookshop.class' = 'batch');

-- The merge. WHEN MATCHED AND ... is the definition of "changed" for every consumer:
-- a source update to a column it ignores is, correctly, not a change.
MERGE INTO ice.bookshop.orders_curated t
USING (SELECT order_id, customer_id, DATE_ADD(DATE '1970-01-01', order_date) AS order_date, status
       FROM ice.bookshop.orders_cdc) s
ON t.order_id = s.order_id
WHEN MATCHED AND (t.status <> s.status OR t.customer_id <> s.customer_id) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- The consumer's watermark. A pushed-down max() on a lineage column fails with
-- "Cannot find field '_last_updated_sequence_number'"; turn aggregate pushdown off for it.
SET spark.sql.iceberg.aggregate-push-down.enabled = false;
SELECT max(_last_updated_sequence_number) AS watermark FROM ice.bookshop.orders_curated;

-- The consumer's delta (a filter on the lineage column is fine):
SELECT order_id, customer_id, order_date, status, _row_id, _last_updated_sequence_number
FROM   ice.bookshop.orders_curated
WHERE  _last_updated_sequence_number > <last watermark>;
