"""Desktop-only Spark adapter. No session or service starts on import."""
from __future__ import annotations

import os
import re
from pathlib import Path

from .catalog import catalog, table_name
from .contracts import Refused
from .fixture import records


def session():
    from pyspark.sql import SparkSession
    required = ("BOOK3_REST_URI", "BOOK3_WAREHOUSE", "BOOK3_CREDENTIAL")
    if any(not os.environ.get(k) for k in required):
        raise Refused("spark_config", "Configure the REST URI, warehouse and a scoped operator credential")
    b = SparkSession.builder.master(os.environ.get("BOOK3_SPARK_MASTER", "local[2]")).appName("book3-agents")
    settings = {
        "spark.ui.enabled": "false", "spark.sql.session.timeZone": "UTC",
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog", "spark.sql.catalog.ice.type": "rest",
        "spark.sql.catalog.ice.uri": os.environ["BOOK3_REST_URI"], "spark.sql.catalog.ice.warehouse": os.environ["BOOK3_WAREHOUSE"],
        "spark.sql.catalog.ice.credential": os.environ["BOOK3_CREDENTIAL"],
        "spark.sql.catalog.ice.scope": os.environ.get("BOOK3_SCOPE", "PRINCIPAL_ROLE:ALL"),
        "spark.sql.catalog.ice.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.ice.cache-enabled": "false",
    }
    if os.environ.get("BOOK3_S3_ENDPOINT"):
        settings["spark.sql.catalog.ice.s3.endpoint"] = os.environ["BOOK3_S3_ENDPOINT"]
        settings["spark.sql.catalog.ice.s3.path-style-access"] = "true"
    for k,v in settings.items():
        b = b.config(k,v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


class SparkBridge:
    def __init__(self, spark, work: Path):
        if os.environ.get("BOOK3_CATALOG") != "rest":
            raise Refused("catalog_mismatch", "Spark and PyIceberg must use the same configured REST catalog")
        self.spark, self.work = spark, work
        self.table = table_name()
        self.qualified = "ice." + self.table

    def identity(self) -> dict:
        table = catalog(self.work).load_table(self.table)
        snap = table.current_snapshot()
        if snap is None:
            raise Refused("empty_history", "Seed the table before staging changes")
        return {"table": self.table, "table_uuid": str(table.metadata.table_uuid), "snapshot_id": snap.snapshot_id}

    def branch_snapshot(self, branch: str) -> int:
        if not re.fullmatch(r"review_[a-f0-9]{32}", branch):
            raise Refused("branch_name", "Only a generated review branch is allowed")
        table = catalog(self.work).load_table(self.table)
        ref = table.metadata.refs.get(branch)
        if ref is None:
            raise Refused("missing_branch", "The review branch is absent")
        return ref.snapshot_id

    def rows_at(self, snapshot_id: int) -> list[dict]:
        if type(snapshot_id) is not int or snapshot_id < 0:
            raise Refused("snapshot_id", "Expected an integer snapshot id")
        return [row.asDict() for row in self.spark.sql(f"SELECT * FROM {self.qualified} VERSION AS OF {snapshot_id}").collect()]

    def stage(self, proposal: dict) -> int:
        if proposal['state'] not in ('proposed','staged'):
            raise Refused('wrong_state','Reviewed evidence cannot be changed by staging')
        if proposal["table_name"] != self.table or self.identity()["table_uuid"] != proposal["table_uuid"]:
            raise Refused("table_changed", "Proposal and configured table differ")
        if self.identity()["snapshot_id"] != proposal["base_snapshot"]:
            raise Refused("base_changed", "Main moved before staging")
        branch = proposal["branch"]
        if not re.fullmatch(r"review_[a-f0-9]{32}", branch):
            raise Refused("branch_name", "Invalid generated branch")
        table = catalog(self.work).load_table(self.table)
        if branch not in table.metadata.refs:
            self.spark.sql(f"ALTER TABLE {self.qualified} CREATE BRANCH {branch} AS OF VERSION {proposal['base_snapshot']}")
        correction = proposal["correction"]
        if type(correction["order_id"]) is not int or type(correction["refund_cents"]) is not int:
            raise Refused("invalid_correction", "Only integer order and refund values are accepted")
        self.spark.sql(f"ALTER TABLE {self.qualified} SET TBLPROPERTIES ('write.wap.enabled'='true')")
        # Explicit branch target; no session-wide read redirection is set.
        self.spark.sql(f"UPDATE {self.qualified}.branch_{branch} SET refund_cents={correction['refund_cents']} WHERE order_id={correction['order_id']}")
        return self.branch_snapshot(branch)

    def fast_forward(self, branch: str, *, expected_uuid: str, expected_base: int, expected_candidate: int):
        from .publication import fast_forward_exact
        fast_forward_exact(self.work, self.table, branch, expected_uuid=expected_uuid,
                           expected_base=expected_base, expected_candidate=expected_candidate)


def seed_rest(spark, *, format_version: int = 2):
    if format_version not in (2,3):
        raise ValueError("Use format version 2 or 3")
    name = table_name()
    ns = name.split('.')[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS ice.{ns}")
    spark.sql(f"CREATE TABLE ice.{name} (order_id BIGINT NOT NULL, tenant STRING NOT NULL, customer_id BIGINT NOT NULL, order_date DATE NOT NULL, status STRING NOT NULL, currency STRING NOT NULL, gross_cents BIGINT NOT NULL, discount_cents BIGINT NOT NULL, refund_cents BIGINT NOT NULL) USING iceberg TBLPROPERTIES ('format-version'='{format_version}')")
    frame = spark.createDataFrame(records(), schema="order_id long, tenant string, customer_id long, order_date date, status string, currency string, gross_cents long, discount_cents long, refund_cents long")
    frame.writeTo("ice."+name).append()


def lineage_image(spark, work: Path) -> dict:
    table = catalog(work).load_table(table_name())
    if table.metadata.format_version != 3:
        raise Refused("requires_v3", "The row-lineage experiment uses a separately seeded V3 table")
    snap = table.current_snapshot()
    rows = spark.sql(f"SELECT _row_id, _last_updated_sequence_number, tenant, order_id, status FROM ice.{table_name()} VERSION AS OF {snap.snapshot_id}").collect()
    image = [{"_row_id": row["_row_id"], "_last_updated_sequence_number": row["_last_updated_sequence_number"],
              "tenant": row["tenant"], "text": f"Order {row['order_id']} has status {row['status']}"} for row in rows]
    return {"table_uuid": str(table.metadata.table_uuid), "snapshot_id": snap.snapshot_id,
            "sequence": snap.sequence_number, "current_rows": image}
