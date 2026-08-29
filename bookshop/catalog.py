"""Two catalogs, because the book uses two.

`local_catalog()` is chapters 1-4: a SQLite file and a directory, no Docker.
`rest_catalog()` is chapter 5 onward: the REST server from docker-compose.
"""
import os
from pathlib import Path

REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
ROOT = Path(__file__).resolve().parent.parent


def local_catalog(name: str = "bookshop", subdir: str = "labs/local"):
    """A spec-conforming catalog with zero infrastructure (chapters 1-4)."""
    from pyiceberg.catalog.sql import SqlCatalog

    base = ROOT / subdir
    (base / "wh").mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        name,
        uri=f"sqlite:///{base / 'catalog.db'}",
        warehouse=f"file://{base / 'wh'}",
    )


def rest_catalog(name: str = "bookshop"):
    """The REST catalog from docker-compose (chapter 5 onward)."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(name, uri=REST_URI)


def duckdb_attached(alias: str = "ice"):
    """DuckDB attached to the same REST catalog (chapter 15).

    AUTHORIZATION_TYPE 'none' is the key. Without it you get an oauth2 error
    that names its own fix. On a real catalog such as Polaris this is the
    wrong answer -- you would supply a secret instead.
    """
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    con.sql(
        f"ATTACH 'warehouse' AS {alias} (TYPE ICEBERG, ENDPOINT '{REST_URI}', "
        f"AUTHORIZATION_TYPE 'none')"
    )
    return con
