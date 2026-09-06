# iceberg-bookshop

The clone-and-run companion to the **Apache Iceberg** books by Simon Sarkar:

| Book | Folder | What it stands up |
|---|---|---|
| *Apache Iceberg from the Ground Up* | `book-1/` | the reference REST catalog on a local directory, 50,000 orders, three verify scripts |
| *Apache Iceberg in the Wild* | `book-2/` | the whole platform: object storage, three catalogs, Trino, Flink, Kafka and Debezium |

An Apache Iceberg lakehouse for a fictional bookshop, plus scripts that reproduce
each book's load-bearing findings on your own machine. Everything is pinned to
the versions the books were written and run against. The `bookshop` package at
the root — the canonical schemas with the field IDs the books print, the seed,
the catalog and Spark helpers — is shared by every book.

**Book 1's commands are unchanged**: run them at the root exactly as its chapters
print them. Book 2's live under `book-2/` (`make -C book-2 help`).

## Quick start

```bash
make venv     # virtualenv + pinned dependencies
make up       # start the REST catalog (Docker), wait for it to answer
make seed     # create and load books, customers, orders
make verify   # run every verification script
```

`make help` lists the rest. `make down` stops the catalog and keeps your data;
`make clean` removes both.

Requires **Docker**, **Python 3.12+**, and a **JDK 17 or 21** for the Spark scripts.

## What gets verified

| Script | Chapters | Reproduces |
|---|---|---|
| `verify/01_anatomy.py` | 1–4 | Every commit adds exactly four files; statistics prune files before they are opened (`order_id > 500` plans **0 of 3**); the catalog is five columns and a commit moves one pointer. No Docker or JVM needed. |
| `verify/02_deletes_and_maintenance.py` | 10–13 | Copy-on-write leaves 994 records in its data files while merge-on-read leaves 1,000 plus a delete file; then a table wrecked by 30 micro-commits is compacted and expired back down, with the query time measured either side. |
| `verify/03_cross_engine.py` | 15 | PyIceberg and DuckDB, pointed at the same catalog, agree on revenue by country to the cent. |

Each script asserts its own result, so `make verify` fails loudly rather than
printing something that looks plausible.

## Pinned versions

| | |
|---|---|
| Apache Iceberg | 1.11.0 |
| PyIceberg | 0.11.1 |
| PySpark | **4.1.3** |
| Iceberg Spark runtime | `iceberg-spark-runtime-4.1_2.13:1.11.0` |
| REST catalog | `apache/iceberg-rest-fixture:1.10.1` |
| DuckDB | 1.5.5 |

### The version trap, up front

`pip install pyspark` currently gives you **4.2.0**, and **no Iceberg runtime exists
for Spark 4.2** — the newest published is `4.1`. The jar coordinate encodes *three*
versions (the Spark minor, the Scala minor, and Iceberg) and all three must line up
with your installed PySpark. That is why `pyproject.toml` pins PySpark exactly, and
it is the most common way a first Iceberg-on-Spark session fails.

Also: do not reach for `tabulario/spark-iceberg`. It is what most tutorials use and
it is several Iceberg releases behind, predating format version 3 entirely.

## Notes on the catalog

The catalog is Apache's **reference REST fixture**, not a production catalog. It is
internally a `JdbcCatalog` backed by SQLite, which you can see for yourself:

```bash
docker compose logs catalog | grep jdbc:sqlite
```

That is the book's point about catalogs in one line: SQLite does not disappear behind
a REST server, it moves behind it.

Two consequences of using the fixture. The warehouse is mounted at the **same absolute
path** inside the container as on the host, because the catalog hands clients the
warehouse path it was configured with and a client on the host has to be able to write
there. And authentication is off, which is why DuckDB attaches with
`AUTHORIZATION_TYPE 'none'` — on a real catalog such as Polaris or Lakekeeper that is
the wrong answer, and you would supply a secret instead.

## Layout

```
bookshop/            shared: catalog helpers, canonical schemas, Spark session, the seed
book-1/              Book 1's docker-compose.yml, verify/ (one script per group of chapters), Makefile
book-2/              Book 2's platform: docker-compose.yml, configs, flink/sql, verify/, Makefile — see book-2/README.md
```

`bookshop/schema.py` holds the field IDs the book prints, so a table you create here
matches the metadata shown in chapter 3.

## Licence

MIT.
