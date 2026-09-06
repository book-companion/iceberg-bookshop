#!/usr/bin/env bash
# Chapter 9: the planning-latency probe. Three measurements, each one line.
#   ./health/planning_probe.sh <namespace> <table>
# Needs the counting proxy (chapter 2's nginx in front of the object store) for the first.
set -u
NS=${1:?namespace}; T=${2:?table}; PREFIX=${BOOK2_PREFIX:-book2}; REST=${BOOK2_REST:-http://localhost:8181}

echo "== 1. what the engine reads to plan (metadata GETs at the proxy, before/after one query)"
before=$(docker exec "$PREFIX-s3-counter" grep -c "GET .*/metadata/" /var/log/nginx/s3.log 2>/dev/null || echo 0)
echo "   run your query now, then re-run this line:  docker exec $PREFIX-s3-counter grep -c 'GET .*/metadata/' /var/log/nginx/s3.log   (was $before)"

echo "== 2. what the catalog serves on every load"
printf '   LoadTable response: %s bytes\n' "$(curl -s "$REST/v1/namespaces/$NS/tables/$T" | wc -c | tr -d ' ')"

echo "== 3. what is accumulating (run in Spark or Trino)"
cat <<SQL
   SELECT count(*) FROM ice.$NS.$T.manifests;            -- flat for a merging writer (Spark); climbing for a fast-append one (Flink)
   SELECT count(*) FROM ice.$NS.$T.snapshots;            -- the metadata file's size, in units of ~1 KB
   SELECT count(*) FROM ice.$NS.$T.metadata_log_entries; -- capped at write.metadata.previous-versions-max; older files are orphans
SQL
