# Certification matrix — 2026-09-05

Catalog: apache/iceberg-rest-fixture 1.10.1 on SeaweedFS 4.45. Iceberg library 1.11.0 (Spark 4.1.3, Flink 2.1.3, Trino 483), PyIceberg 0.11.1, DuckDB 1.5.5. Rows: id, ts (naive), tstz, date, decimal(10,2), string, boolean; three rows, decimal sum 60.00; every timestamp is 2026-06-01 12:00:00 (UTC). Session time zones set to UTC where the engine has one.

## Writers: create + insert 3 rows

| Writer | v2 | v3 |
|---|---|---|
| spark | OK | OK |
| duckdb | OK | OK |
| trino | OK | OK |
| flink | OK | OK |
| pyiceberg | OK | Cannot write manifest for table version: 3 |

## Readers, before any delete (expect n=3, sum=60.00, both timestamps 2026-06-01 12:00:00)

| Table (writer_vN) | spark | duckdb | trino | pyiceberg | flink |
|---|---|---|---|---|---|
| duckdb_v2 | OK | OK | OK | OK | OK |
| duckdb_v3 | OK | OK | OK | OK | OK |
| flink_v2 | OK | OK | OK | OK | OK |
| flink_v3 | OK | OK | OK | OK | OK |
| pyiceberg_v2 | OK | OK | OK | OK | OK |
| spark_v2 | OK | OK | OK | OK | OK |
| spark_v3 | OK | OK | OK | OK | OK |
| trino_v2 | OK | OK | OK | OK | OK |
| trino_v3 | OK | OK | OK | OK | OK |

## After `DELETE WHERE id = 2` by Spark (expect n=2, sum=40.00)

| Table | delete by Spark | spark | duckdb | trino | pyiceberg | flink |
|---|---|---|---|---|---|---|
| duckdb_v2 | OK | OK | OK | OK | OK | OK |
| duckdb_v3 | OK | OK | OK | OK | OK | OK |
| flink_v2 | OK | OK | OK | OK | OK | OK |
| flink_v3 | OK | OK | OK | OK | OK | OK |
| pyiceberg_v2 | OK | OK | OK | OK | OK | OK |
| spark_v2 | OK | OK | OK | OK | OK | OK |
| spark_v3 | OK | OK | OK | OK | OK | OK |
| trino_v2 | OK | OK | OK | OK | OK | OK |
| trino_v3 | OK | OK | OK | OK | OK | OK |

## Delete files Spark produced

| Table | delete files (content, count) | paths |
|---|---|---|
| duckdb_v2 | content=1 x1 | ae7db7-00001-deletes.parquet |
| duckdb_v3 | content=1 x1 | ea17830-00001-deletes.puffin |
| flink_v2 | content=1 x1 | 35091c-00001-deletes.parquet |
| flink_v3 | content=1 x1 | 587e680-00001-deletes.puffin |
| pyiceberg_v2 | content=1 x1 | 4c1bfe-00001-deletes.parquet |
| spark_v2 | content=1 x1 | cbc2d0-00001-deletes.parquet |
| spark_v3 | content=1 x1 | ea8e590-00001-deletes.puffin |
| trino_v2 | content=1 x1 | f07c88-00001-deletes.parquet |
| trino_v3 | content=1 x1 | a0a5a08-00001-deletes.puffin |

## Views (Spark-dialect SQL stored in the catalog)

| Reader | result |
|---|---|
| spark | 2 |
| trino | ERR: Query 20260906_063343_00067_hmw3k failed: Cannot read unsupported dialect 'spark' for view |
| duckdb | ERR: Error: Table with name v_spark does not exist! Did you mean "spark_v3"?  LINE 1: SELECT co |
| flink | ERR: org.apache.calcite.sql.validate.SqlValidatorException: Object 'v_spark' not found within ' |
| pyiceberg | True |
