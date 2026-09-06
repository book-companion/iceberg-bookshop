# Book 2 — Apache Iceberg in the Wild

The platform the book is written against, in one compose file, and the scripts
that reproduce its findings. Run from this directory, or `make -C book-2 <target>`
from the repository root (the root `make venv` builds the one virtualenv both books
share).

```bash
make up        # fetches Flink's jars, starts everything, waits for the catalogs, Trino and Flink
make verify    # every verification script, each asserting its own result
make down      # stop, keep the data
```

Requires **Docker** with about **10 GB** of memory available to it (the whole
platform measured ~8.2 GB resident), **Python 3.13**, and a **JDK 21** for Spark
on the host.

## What comes up

| Service | Image | Host port | Chapter that introduces it |
|---|---|---|---|
| SeaweedFS (S3-compatible object store, `mini` mode, IAM + STS) | `chrislusf/seaweedfs:4.45` | 8333 | 1, 2 |
| Reference REST catalog, on object storage | `apache/iceberg-rest-fixture:1.10.1` | 8181 | 1 |
| PostgreSQL 18 (catalog databases and the bookshop source) | `postgres:18` | 5432 | 3 |
| Apache Polaris | `apache/polaris:1.7.0` | 8281, 8282 | 3 |
| Lakekeeper (joined to a socat forwarder's network namespace, so its one storage endpoint is valid everywhere) | `quay.io/lakekeeper/catalog:v0.13.3` | 8381 | 3 |
| Trino | `trinodb/trino:483` | 8080 | 5 |
| Flink (jobmanager + taskmanager) | `flink:2.1.3-scala_2.12-java21` + Iceberg 1.11.0 runtime | 8081 | 11 |
| Kafka (KRaft, one node) | `apache/kafka:4.3.1` | 29092 | 11, 12 |
| Debezium Connect | `quay.io/debezium/connect:3.6.2.Final` | 8083 | 12 |

Host clients (Spark, PyIceberg, DuckDB) reach the object store at
`localhost:8333`; containers reach it as `seaweedfs:8333`. Same store, two
addresses — chapter 2 is about why that matters and chapter 3 about what each
catalog does with it. Every host client sets its own `s3.endpoint`.

## Why not MinIO

MinIO stopped publishing Docker images in September 2025 and archived its
repository in April 2026. SeaweedFS was chosen after a four-way probe of
S3-compatible stores on the calls Iceberg actually makes, including conditional
writes; chapter 2 shows the probe and the one store that returned success for a
condition it did not honour.

## Files

| | |
|---|---|
| `docker-compose.yml` | the platform, commented stage by stage |
| `seaweedfs-iam.json` | the lab key and the STS role the catalogs vend credentials through |
| `postgres-init.sql` | the three databases: `polaris`, `lakekeeper`, `bookshop` |
| `trino/catalog/*.properties` | one Trino catalog per way of reaching the tables |
| `flink/sql/*.sql` | Flink SQL for the upsert, streaming and CDC runs (`sql-client.sh -f /scripts/<file>` inside the jobmanager) |
| `fetch-jars.sh` | the Flink runtime, AWS bundle, Hadoop client and Kafka connector jars, from Maven Central |
| `verify/01_platform.py` | chapter 1: version manifest, then four engines agreeing on one table |
| `nginx/s3-counter.conf` | the counting proxy in front of the object store (chapters 2 and 6): every S3 request a client makes, logged |
| `bench/table_design.py` | chapter 6: three workloads built two ways, measured under the pre-warm / interleave / median protocol |
| `compatibility/` | chapter 5: the generated engine certification matrix, dated, regenerated on every version bump |
