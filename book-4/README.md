# The Apache Iceberg Migration Playbook — companion draft

The sixteen chapters use one small bookshop pilot, then alternative source
routes and a common validation/cutover path. The test suite (`make test`, 12 tests)
and the local probes (`pilot`, `raw`, `boundary`, `cutover`) pass on macOS with
Python 3.13.5; the pilot reconciles eight orders to 39,000 cents. **The platform,
source-route and Spark experiments are unrun**: Hive, Delta, Hudi, XTable,
PostgreSQL, live CDC, the REST target and the layout benchmark. No benchmark winner
or source-format compatibility result is supplied as if it had been observed.

## First pilot

From this directory, using Python 3.13:

```bash
make setup
.venv/bin/book4 --work work/first-pilot pilot
```

The command writes an immutable eight-order Parquet source, captures its inventory
and report baseline, creates a real local Iceberg V2 table, compares exact rows
and four consumer queries, and records interleaved timing samples. The authored
revenue oracle is 39,000 USD cents overall and 21,000 for north. Cancelled and
pending rows contribute nothing. These numbers are assertions, not observed output.

Use a fresh work directory for each attempt. Commands refuse to overwrite prior
evidence. `work/` contains generated data, catalogs and reports; it is ignored.
The existing Book 1/2 environment and services are not modified by local exercises.

## Deterministic checks and public probes

```bash
.venv/bin/python -m pytest tests/test_contracts.py
.venv/bin/python -m pytest tests/test_local_iceberg.py
.venv/bin/python -m iceberg_migration.probes --scenario raw --work work/raw-first
.venv/bin/python -m iceberg_migration.probes --scenario boundary --work work/boundary-first
.venv/bin/python -m iceberg_migration.probes --scenario cutover --work work/cutover-first
```

The raw-zone exercise retains two quarantined files and copies eight accepted
rows. A passing quarantine check does not make the whole source release eligible.
The boundary exercise tests a declared contiguous single event stream in SQLite;
its integer positions are not PostgreSQL LSNs or Kafka offsets. The cutover
exercise actually changes isolated local data and routing state, but it does not
fence an external application or distributed writer.

## Evidence tools

```bash
.venv/bin/book4 inventory work/first-pilot/source
.venv/bin/book4 reconcile work/first-pilot/source.json work/first-pilot/target.json \
  --boundary 'immutable authored fixture'
.venv/bin/book4 consumers work/first-pilot/source.json work/first-pilot/target.json
.venv/bin/book4 wave templates/portfolio.json
.venv/bin/book4 versions
```

Reconciliation checks packet integrity, schema mapping, duplicate keys, exact
row multisets and business totals. A logical boundary is mandatory, but the
comparison does not establish it: the capture procedure must do that. Exact
comparison is intentionally small-pilot code and materializes rows in memory.
Scale it through partitioned evidence with equivalent semantics before applying
it to a large estate.

Consumer comparison preserves duplicates and declared ordering. The local route
uses DuckDB over captured images; the PostgreSQL offload also compares actual
PostgreSQL report results with DuckDB. Neither certifies a BI application's
refresh/cache behavior. `templates/consumer-cutover.md` supplies that handoff.

## Source and platform routes

Follow [DESKTOP.md](DESKTOP.md). Source routes are alternatives, not mandatory
installations for every reader. Target Python dependencies are in `pyproject.toml`;
they are not a resolved freeze. Spark targets 4.1.3 with Iceberg 1.11.0 and JDK 21.

| Scenario | Prerequisites and authored scope |
|---|---|
| `target` | Scoped REST catalog/storage configuration; fresh Spark write and PyIceberg read |
| `policy` | Valid operator/restricted identities and two existing control tables |
| `hive` | Owned HMS 4.1.0; shared local work mount; snapshot/migrate/add-files hazards |
| `delta` | Fresh JVM; target `io.delta:delta-spark_4.1_2.13:4.1.0`; update/delete/current-state rewrite |
| `hudi` | Fresh JVM; target `org.apache.hudi:hudi-spark4.1-bundle_2.13:1.2.0`; COW update/delete/current-state rewrite |
| `xtable` | Operator-selected bundled jar and SHA256; disposable successful Delta/Hudi source fixture |
| `postgres` | Owned PostgreSQL 18 `sqlverify` database; repeatable-read export and report comparison |
| `layout` | Small local Spark partition and sort comparisons, actual plans and interleaved timings |
| `cdc` | Existing Book 2 convergence check, read-only from this wrapper |
| `cdc-prepare` / `cdc-live` | Owned four-column source and reused Book 2 pipeline; actual overlap and exact current-image handoff |

XTable remains gated on the selected artifact and feature cases. It is not
assumed compatible merely because Delta and Hudi packages exist. Current-state
rewrites begin new Iceberg history; source-native histories, deletion vectors,
MOR logs, change feeds and engine-specific policies require separate qualification.

## Module map

| Chapters | Implementation |
|---|---|
| 1–4 | `fixture`, `inventory`, `reconcile`, `policy`, charter/mapping templates |
| 5–8 | `hive`, `parquet`, `formats`, `postgres`, `spark` |
| 9–10 | mapping rules, `consumers`, `benchmark`, layout probe, consumer cutover worksheet |
| 11 | `boundary`, `cdc_bridge`, `cdc_live`; existing Book 2 SQL and convergence code |
| 12–14 | `reconcile`, `policy`, `cutover`; exact failures and recovery tests |
| 15–16 | `benchmark`, `portfolio`; accumulated evidence and wave planning |

The wave planner compares recorded evidence claims; it cannot authenticate a
forged artifact or approve a migration. Operators must review provenance, policy
and actual writer controls. Optional agent assistance can summarize these inputs
but has no approval capability in this companion.
