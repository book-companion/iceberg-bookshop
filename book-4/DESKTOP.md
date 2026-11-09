# Book 4 desktop execution order

These are unrun instructions for a desktop with enough memory for the selected
profiles. A 64 GiB host is the planned machine, not a measured resource guarantee.
Run one profile at a time initially. Keep every actual failure and dependency
resolution; do not turn expected fixture arithmetic into a captured result.

## 1. Isolated environment and local controls

From `book-4/`, target Python 3.13 and JDK 21:

```bash
make setup
.venv/bin/python -m pip install -e '.[spark,postgres]'
.venv/bin/book4 versions
.venv/bin/python -m pip freeze
.venv/bin/python -m pytest tests/test_contracts.py
.venv/bin/python -m pytest tests/test_local_iceberg.py
.venv/bin/book4 --work work/desktop-pilot pilot
.venv/bin/python -m iceberg_migration.probes --scenario raw --work work/desktop-raw
.venv/bin/python -m iceberg_migration.probes --scenario boundary --work work/desktop-boundary
.venv/bin/python -m iceberg_migration.probes --scenario cutover --work work/desktop-cutover
```

Capture the resolved freeze and actual versions after installation. The direct
pins have not been resolved here. The raw profile deliberately quarantines two
files. A successful experiment proves that quarantine blocks release; it does
not approve dropping those source records.

## 2. Shared target, separate policy controls

Start/bootstrap Book 2's existing platform according to its README. Check actual
host/Docker resources and occupied ports first. Never run shared `down` or `clean`
as part of these exercises. Configure privately:

- `MIGR_REST_URI`, `MIGR_WAREHOUSE`, `MIGR_S3_ENDPOINT`;
- `MIGR_CREDENTIAL`: operator restricted to owned `migr_*` sandbox scope;
- optional `MIGR_SCOPE`, default `PRINCIPAL_ROLE:ALL`;
- `MIGR_RESTRICTED_CREDENTIAL`: a valid separately scoped principal;
- `MIGR_ALLOWED_CONTROL`: existing table with at least one readable row;
- `MIGR_FORBIDDEN_CONTROL`: operator-readable table the restricted principal
  must be denied loading.

Provision grants through the real catalog administration route. Do not replace
shared principals to make a probe convenient. The positive read must succeed
before a `ForbiddenError` can count as the intended table-scope denial.

```bash
.venv/bin/python -m iceberg_migration.probes --scenario target --work work/desktop-target
.venv/bin/python -m iceberg_migration.probes --scenario policy --work work/desktop-policy
```

These test Spark/PyIceberg data compatibility and a PyIceberg REST policy path.
Other engine, view, row and column-policy paths need separate recorded cases in
the policy matrix. No policy parity is inferred from equal row counts.

## 3. Select a source route

### PostgreSQL 18 warehouse

The owned Compose project uses `sqlverify` on host port 5544, separate from
Book 2's PostgreSQL port. Set `MIGR_POSTGRES_PASSWORD` privately, then:

```bash
docker compose -p migration -f docker-compose.yml up -d warehouse
docker compose -p migration -f docker-compose.yml ps
```

Set `MIGR_POSTGRES_DSN` privately for user `migration`, database `sqlverify`, host
`localhost`, port `5544` and the chosen password. The offload creates a new
`migr_*` schema; it never replaces an existing source table.

```bash
.venv/bin/python -m iceberg_migration.probes --scenario postgres --work work/desktop-postgres
```

The export and source reports use one repeatable-read transaction. This is not
a replication-slot or WAL handoff guarantee. The source schema remains available
for diagnosis and later explicit cleanup. Record the actual PostgreSQL image digest.

### Hive Metastore

The metastore uses its own project and port 19083. It is a disposable Derby-backed
source service. Set an absolute owned work root, mounted at the identical host
and container path so both Spark and the metastore can resolve local locations:

```bash
mkdir -p work
export MIGR_HIVE_WORKSPACE="$PWD/work"
export MIGR_HMS_URI='thrift://localhost:19083'
docker compose -p migration -f docker-compose.yml --profile hive up -d metastore
.venv/bin/python -m iceberg_migration.probes --scenario hive --work work/desktop-hive
```

Confirm that the metastore process can access the mounted directory. If platform
permissions or client/server compatibility fail, retain that failure and repair
the isolated setup; do not mount the shared Book 2 warehouse as a workaround.
The private lab uses this same work root for its Hive attempts when configured.

The probe creates owned external Parquet fixtures, snapshots shared files, moves
one owned file temporarily to demonstrate the hazard, and restores it in a
`finally` block. A forced process termination can interrupt that restoration;
inspect `.held` files only inside the reported fixture before retrying. It also
migrates a separate table and compares GZIP source files with an explicit ZSTD
append. Duplicate registration is deliberately enabled only in a disposable table.

### Delta and Hudi feasibility

Each command starts a fresh JVM with one source-format dependency set:

```bash
.venv/bin/python -m iceberg_migration.probes --scenario delta --work work/desktop-delta
.venv/bin/python -m iceberg_migration.probes --scenario hudi --work work/desktop-hudi
```

Target coordinates are recorded in `spark.py`. A package resolving successfully
is not sufficient: update, delete, source read and current-state Iceberg rewrite
must satisfy the exact assertions. Hudi uses a source-only monotonic ordering
field; the business projection excludes it. COW is the selected Hudi profile.
No MOR or deletion-vector compatibility is inferred from it.

### XTable qualification

Select a specific bundled XTable artifact from the project's supported build
route. Record its version or source commit, build instructions and independently
calculated SHA256. Set `MIGR_XTABLE_JAR` and `MIGR_XTABLE_SHA256`; the draft does
not invent a tested jar version. Review the current feature limitations before
running translation on the disposable source produced above.

```bash
.venv/bin/python -m iceberg_migration.probes --scenario xtable \
  --work work/desktop-xtable \
  --source-path work/desktop-delta/delta --source-format DELTA \
  --expected work/desktop-delta/source.json
```

The source must carry the successful format probe's marker. Translation changes
metadata beside those source files. A current-row match does not establish source
history, change-feed, generated-column or policy equivalence. Source retention
still governs shared files. Keep the route blocked if the selected artifact or
feature contract cannot be qualified.

## 4. Reuse Book 2 CDC for a migration boundary

The deterministic `boundary` scenario is independent of the real pipeline. For
the live overlap experiment, keep Book 2's Kafka, Debezium and Flink services
running and prepare an owned source/configuration set:

```bash
.venv/bin/python -m iceberg_migration.probes --scenario cdc-prepare --work work/cdc-plan
```

This creates eight rows in a generated PostgreSQL schema and writes
`cdc-manifest.json`, `connector-template.json` and `flink.sql`. The SQL derives
from Book 2's pipeline with new namespace/topic/group identifiers and its drop
statement removed. Preparation does not submit a connector or job.

Review both configurations. The connector expects the source at
`host.docker.internal:5544`; use the actual host gateway address if the desktop's
container runtime requires another route. The source password placeholder must
be supplied privately. One submission method keeps it out of a committed file:

```python
# Run from book-4 only after reviewing work/cdc-plan/connector-template.json.
import json, os, urllib.request, urllib.error
from pathlib import Path
payload = json.loads(Path('work/cdc-plan/connector-template.json').read_text())
payload['config']['database.password'] = os.environ['MIGR_POSTGRES_PASSWORD']
base = 'http://localhost:8083/connectors'
try:
    urllib.request.urlopen(base + '/' + payload['name'])
except urllib.error.HTTPError as error:
    if error.code != 404:
        raise
else:
    raise RuntimeError('This connector already exists; inspect it instead of replacing it')
request = urllib.request.Request(base, data=json.dumps(payload).encode(),
    headers={'Content-Type': 'application/json'}, method='POST')
with urllib.request.urlopen(request) as response:
    print('Connector created:', payload['name'], 'HTTP', response.status)
```

Use the actual Book 2 jobmanager container name (the public default is
`book2-flink-jm`) and retain the returned job id:

```bash
docker cp work/cdc-plan/flink.sql book2-flink-jm:/tmp/migration-cdc.sql
docker exec book2-flink-jm ./bin/sql-client.sh -f /tmp/migration-cdc.sql
.venv/bin/python -m iceberg_migration.probes --scenario cdc-live \
  --work work/desktop-cdc-live --manifest work/cdc-plan/cdc-manifest.json
```

The live exercise changes the owned source while a repeatable-read backfill
snapshot is open. It reuses Book 2's convergence function with the owned source
adapter, then compares exact landing rows. It deliberately omits deletes from
one merge, detects the mismatch, repairs the merge and repeats it. It uses Book
2's reference REST catalog path, separately from the Polaris policy probes.

This establishes only the exercised current-image handoff. It does not claim
that a database snapshot string is a WAL position or that a connector restart
was injected. Before production, rehearse restart/savepoint behavior using Book
2's procedures on this owned job and reconcile again. Never resume a savepoint
twice into the same target merely to see whether duplicates appear.

Stop/delete only this connector and the recorded owned Flink job when finished.
Remove its replication slot/publication only after confirming the connector is
stopped and retained evidence no longer needs them. Shared services remain up.

## 5. Performance, consumers and final evidence

```bash
.venv/bin/python -m iceberg_migration.probes --scenario layout --work work/desktop-layout
.venv/bin/book4 wave templates/portfolio.json
```

The layout profile uses 4,000 authored rows, warm-up and seven interleaved trials.
It captures plans and file counts/bytes. It is a small local comparison, not a
production speedup claim. Add representative scale/concurrency before using the
result to choose a deployment layout. A third day-partitioned table has a tenant/order-id write order; a separate
key-range query compares sorting with seven interleaved trials. Do not attribute
partition-query results to sorting. Query result ordering remains explicit.

Complete the consumer worksheet for actual dbt/jobs/BI clients and the full
policy matrix for intended engines. Keep unknown owners, quarantines, unsupported
features, stale evidence and untested recovery paths blocked. Retain source
commits, full environment freeze, actual service image digests, commands,
return codes and reviewed artifacts. No new model call is required for Book 4.
