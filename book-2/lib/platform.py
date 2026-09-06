"""Shared plumbing for the Book 2 scripts: a Spark session on the reference REST
catalog, an object-store client, a request counter (if the counting proxy from
chapter 2 is up), and a psql helper for the bookshop source.

Every host client sets its own s3.endpoint: the catalogs were configured with
http://seaweedfs:8333, a name only containers resolve.

Environment (all optional):
  BOOK2_PREFIX   container-name prefix, default `book2`
  BOOK2_REST     REST catalog URI, default http://localhost:8181
  BOOK2_S3       object-store endpoint for host clients, default http://localhost:8333
  BOOK2_PROXY    the counting proxy, default http://localhost:8334 (used when reachable)
"""
import os, subprocess, urllib.request

PREFIX = os.environ.get("BOOK2_PREFIX", "book2")
REST = os.environ.get("BOOK2_REST", "http://localhost:8181")
S3 = os.environ.get("BOOK2_S3", "http://localhost:8333")
PROXY = os.environ.get("BOOK2_PROXY", "http://localhost:8334")
AK, SK, REGION = "lab-access-key", "lab-secret-key", "us-east-1"
os.environ.update(AWS_ACCESS_KEY_ID=AK, AWS_SECRET_ACCESS_KEY=SK, AWS_REGION=REGION)
os.environ.pop("SPARK_HOME", None)   # a stale SPARK_HOME hijacks PySpark's own distribution


def proxy_up():
    try:
        urllib.request.urlopen(PROXY + "/", timeout=2); return True
    except Exception:
        return False


def s3_endpoint_for_spark():
    return PROXY if proxy_up() else S3


def spark(app="book2", extra=None, cores=4):
    """A Spark 4.1 session with catalog `ice` on the reference REST catalog."""
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master(f"local[{cores}]").appName(app).config("spark.ui.enabled", "false")
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.ice.type", "rest")
         .config("spark.sql.catalog.ice.uri", REST).config("spark.sql.catalog.ice.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.ice.s3.endpoint", s3_endpoint_for_spark()).config("spark.sql.catalog.ice.s3.path-style-access", "true")
         .config("spark.sql.catalog.ice.client.region", REGION).config("spark.sql.catalog.ice.cache-enabled", "false"))
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    s = b.getOrCreate(); s.sparkContext.setLogLevel("ERROR"); return s


def s3client(endpoint=None):
    import boto3
    return boto3.client("s3", endpoint_url=endpoint or S3, aws_access_key_id=AK, aws_secret_access_key=SK, region_name=REGION)


def keys(client, bucket, prefix):
    out, tok = [], None
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix)
        if tok: kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw); out += [o["Key"] for o in r.get("Contents", [])]; tok = r.get("NextContinuationToken")
        if not tok: return out


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def psql(sql, db="bookshop"):
    return docker("exec", f"{PREFIX}-postgres", "psql", "-U", "postgres", "-d", db, "-At", "-c", sql).stdout.strip()


def proxy_log():
    return docker("exec", f"{PREFIX}-s3-counter", "cat", "/var/log/nginx/s3.log").stdout.splitlines()


def counted(fn, kind="data"):
    """Run fn and return (result, GETs under /<kind>/, bytes) as the proxy saw them; (result, None, None) without the proxy."""
    if not proxy_up():
        return fn(), None, None
    before = len(proxy_log()); out = fn(); new = proxy_log()[before:]
    gets = [l for l in new if l.startswith("GET") and f"/{kind}/" in l]
    return out, len(gets), sum(int(l.split()[3]) for l in gets)
