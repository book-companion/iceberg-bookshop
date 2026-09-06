#!/usr/bin/env bash
# Fetch the jars Flink needs from Maven Central into ./flink/lib (gitignored;
# rerun after a clone). Pins: Iceberg 1.11.0's Flink 2.1 runtime — the newest
# Flink line Iceberg 1.11.0 ships a runtime for (Flink itself is at 2.3).
# The Iceberg Flink runtime needs Hadoop's Configuration class on the
# classpath even for a REST catalog on S3FileIO: without it the first
# CREATE CATALOG fails with ClassNotFoundException: org.apache.hadoop.conf.Configuration.
set -euo pipefail
mkdir -p "$(dirname "$0")/flink/lib" && cd "$(dirname "$0")/flink/lib"
M=https://repo1.maven.org/maven2
for a in org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar \
         org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
         org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar \
         org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar \
         org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar; do
  f=$(basename "$a"); [ -s "$f" ] || curl -fsSL -o "$f" "$M/$a"; printf '%-44s %s\n' "$f" "$(du -h "$f" | cut -f1)"
done
