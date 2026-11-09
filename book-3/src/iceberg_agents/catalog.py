"""Catalog selection is trusted process configuration, never a tool argument."""
from __future__ import annotations

import os
from pathlib import Path

from .contracts import Refused


def catalog(work: Path):
    mode = os.environ.get("BOOK3_CATALOG", "local")
    if mode == "local":
        from .fixture import local_catalog
        return local_catalog(work)
    if mode != "rest":
        raise Refused("catalog_config", "BOOK3_CATALOG must be local or rest")
    from pyiceberg.catalog import load_catalog
    required = ("BOOK3_REST_URI", "BOOK3_WAREHOUSE", "BOOK3_CREDENTIAL")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise Refused("catalog_config", "Missing trusted configuration: " + ", ".join(missing))
    config = {
        "type": "rest", "uri": os.environ["BOOK3_REST_URI"],
        "warehouse": os.environ["BOOK3_WAREHOUSE"], "credential": os.environ["BOOK3_CREDENTIAL"],
        "scope": os.environ.get("BOOK3_SCOPE", "PRINCIPAL_ROLE:ALL"),
    }
    if os.environ.get("BOOK3_S3_ENDPOINT"):
        config["s3.endpoint"] = os.environ["BOOK3_S3_ENDPOINT"]
    # The catalog vends storage credentials. No admin-key fallback is supplied.
    return load_catalog("book3", **config)


def table_name() -> str:
    import re
    name = os.environ.get("BOOK3_TABLE", "agents_demo.orders")
    if not re.fullmatch(r"agents_[a-z0-9_]+\.orders", name):
        raise Refused("table_config", "Only an agents_* orders table is accepted")
    return name
