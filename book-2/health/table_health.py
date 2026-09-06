"""Chapter 13: the table-health pack.

Eighteen metrics per table from the metadata tables plus one HEAD on the store and
one GET on the catalog, run against every table in the namespaces given, printed as
one row per table, and optionally written to an `ops.table_health` Iceberg table so
the rules' own accuracy is a query.

  python health/table_health.py <namespace> [<namespace> ...] [--store]

Needs `make -C book-2 up`. About a second per table; hourly is the right schedule.
Freshness is the one per-minute metric and is one query on its own (see rules.sql).
"""
import sys, time, urllib.request
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from lib.platform import spark, s3client, REST

PACK = {
 "snapshots":         "SELECT count(*) FROM {t}.snapshots",
 "since_compaction":  "SELECT count(*) FROM {t}.snapshots WHERE committed_at > coalesce((SELECT max(committed_at) FROM {t}.snapshots WHERE operation = 'replace' AND coalesce(CAST(summary['deleted-data-files'] AS INT), 0) > 0), timestamp'1970-01-01')",
 "flink_written":     "SELECT CASE WHEN summary['flink.job-id'] IS NOT NULL THEN 1 ELSE 0 END FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1",
 "files":             "SELECT count(*) FROM {t}.data_files",
 "partitions":        "SELECT count(*) FROM {t}.partitions",   # one row for an unpartitioned table; count(DISTINCT partition) on data_files fails there
 "median_file_kb":    "SELECT percentile_approx(file_size_in_bytes, 0.5) / 1024 FROM {t}.data_files",
 "delete_files":      "SELECT count(*) FROM {t}.delete_files",
 "delete_rows":       "SELECT coalesce(sum(record_count), 0) FROM {t}.delete_files",
 "manifests":         "SELECT count(*) FROM {t}.manifests",
 "refs":              "SELECT count(*) FROM {t}.refs",
 "last_commit":       "SELECT max(committed_at) FROM {t}.snapshots",
 "gap_median_s":      "SELECT percentile_approx(gap, 0.5) FROM (SELECT (unix_timestamp(committed_at) - unix_timestamp(lag(committed_at) OVER (ORDER BY committed_at))) AS gap FROM {t}.snapshots) WHERE gap IS NOT NULL",
 "gap_p95_s":         "SELECT percentile_approx(gap, 0.95) FROM (SELECT (unix_timestamp(committed_at) - unix_timestamp(lag(committed_at) OVER (ORDER BY committed_at))) AS gap FROM {t}.snapshots) WHERE gap IS NOT NULL",
 "metadata_log":      "SELECT count(*) FROM {t}.metadata_log_entries",
}
COLUMNS = ["run_at", "catalog", "tbl", "class"] + list(PACK) + ["excess_files", "deletes_per_file", "metadata_kb", "freshness_min", "load_kb", "load_ms"]


def health(sql, s3, ns, t):
    q = f"ice.{ns}.{t}"; row = {"run_at": time.strftime("%Y-%m-%d %H:%M:%S"), "catalog": "ice", "tbl": f"{ns}.{t}"}
    props = {r[0]: r[1] for r in sql(f"SHOW TBLPROPERTIES {q}")}
    row["class"] = props.get("bookshop.class", "unclassified")    # declared, never inferred (chapter 13)
    for k, s in PACK.items():
        try: row[k] = sql(s.format(t=q))[0][0]
        except Exception: row[k] = None
    row["excess_files"] = (row["files"] or 0) - (row["partitions"] or 0)
    row["deletes_per_file"] = (row["delete_rows"] or 0) / max(row["files"] or 1, 1)
    meta = sql(f"SELECT file FROM {q}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1")[0][0]
    bucket, key = meta.split("://", 1)[1].split("/", 1)
    row["metadata_kb"] = s3.head_object(Bucket=bucket, Key=key)["ContentLength"] / 1024
    row["freshness_min"] = (time.time() - row["last_commit"].timestamp()) / 60 if row["last_commit"] else None
    t0 = time.perf_counter(); body = urllib.request.urlopen(f"{REST}/v1/namespaces/{ns}/tables/{t}", timeout=30).read()
    row["load_kb"], row["load_ms"] = len(body) / 1024, (time.perf_counter() - t0) * 1000
    return row


def main(argv):
    store = "--store" in argv; namespaces = [a for a in argv if not a.startswith("--")] or ["bookshop"]
    s = spark("table-health"); sql = lambda q: s.sql(q).collect(); s3 = s3client(); rows = []
    t0 = time.perf_counter()
    for ns in namespaces:
        for r in sql(f"SHOW TABLES IN ice.{ns}"):
            rows.append(health(sql, s3, ns, r[1]))
    print(f"{len(rows)} tables in {time.perf_counter()-t0:.0f}s")
    hdr = ["tbl", "class", "snapshots", "since_compaction", "files", "partitions", "excess_files", "median_file_kb", "delete_files", "deletes_per_file", "manifests", "metadata_kb", "load_kb", "freshness_min", "flink_written"]
    print("  " + "".join(f"{h[:12]:>13}" for h in hdr))
    for r in sorted(rows, key=lambda r: r["tbl"]):
        print("  " + "".join(f"{(str(r[h])[:12] if isinstance(r[h], str) else f'{(r[h] or 0):.0f}'):>13}" for h in hdr))
    if store and rows:
        sql("CREATE NAMESPACE IF NOT EXISTS ice.ops")
        sql("""CREATE TABLE IF NOT EXISTS ice.ops.table_health (
          run_at STRING, catalog STRING, tbl STRING, class STRING, snapshots BIGINT, since_compaction BIGINT, flink_written INT,
          files BIGINT, partitions BIGINT, median_file_kb DOUBLE, delete_files BIGINT, delete_rows BIGINT, manifests BIGINT, refs BIGINT,
          last_commit TIMESTAMP, gap_median_s DOUBLE, gap_p95_s DOUBLE, metadata_log BIGINT, excess_files BIGINT, deletes_per_file DOUBLE,
          metadata_kb DOUBLE, freshness_min DOUBLE, load_kb DOUBLE, load_ms DOUBLE) USING iceberg""")
        df = s.createDataFrame([{c: r.get(c) for c in COLUMNS} for r in rows])
        df.writeTo("ice.ops.table_health").append(); print(f"stored {len(rows)} rows in ice.ops.table_health")
    s.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
