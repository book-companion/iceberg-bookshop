# Desktop verification route — Book 3 draft

All commands below are unrun instructions. Run from `book-3/`; use fresh work
directories. No probe stops or cleans the shared Book 2 platform.

## Local environment and deterministic checks

Target Python 3.13 and JDK 21 for Spark. Resolve the isolated environment first:

```bash
make setup-agent
make setup-spark
.venv/bin/book3 versions
.venv/bin/python -m pip freeze
.venv/bin/python -m pytest tests/test_contracts.py
.venv/bin/python -m pytest tests/test_local_iceberg.py
.venv/bin/python -m iceberg_agents.probes --scenario protocol --work work/desktop-protocol
.venv/bin/python -m iceberg_agents.probes --scenario failures --work work/desktop-failures
```

Keep the full resolved freeze with actual results. Direct pins are targets.
The deterministic oracle is 39,000 USD cents overall and 21,000 for north.
Contract doubles, local Iceberg commits and real MCP calls have different scopes.

## Shared platform and scoped identities

Follow Book 2's platform README to start and bootstrap its existing services once.
Check available host and Docker resources first. Do not use shared `down`/`clean`.
Configure `BOOK3_CATALOG=rest`, `BOOK3_REST_URI`, `BOOK3_WAREHOUSE`,
`BOOK3_CREDENTIAL`, `BOOK3_S3_ENDPOINT` and optional `BOOK3_SCOPE`. The operator
credential needs create/read/write/branch privileges in the owned sandbox only.
The REST reader relies on credential vending; it has no broad storage-key fallback.

For the identity probe, the platform operator provisions these prerequisites:

| Setting | Required state |
|---|---|
| `BOOK3_ALLOWED_CONTROL` | Existing table readable by operator and restricted principal; at least one row |
| `BOOK3_FORBIDDEN_CONTROL` | Existing table readable by operator; restricted principal denied table load |
| `BOOK3_RESTRICTED_CREDENTIAL` | Valid separately scoped client-id:secret |

Use Polaris's namespace/table grants through the platform's administration route.
Do not reuse a bootstrap that replaces a shared principal. Verify the allowed
control first. An invalid credential, missing table or unavailable service does
not count as a policy denial. Application tenant policy is tested separately.

```bash
.venv/bin/python -m iceberg_agents.probes --scenario platform --work work/desktop-platform
.venv/bin/python -m iceberg_agents.probes --scenario identity --work work/desktop-identity
.venv/bin/python -m iceberg_agents.probes --scenario lineage --work work/desktop-lineage
.venv/bin/python -m iceberg_agents.probes --scenario health --work work/desktop-health
.venv/bin/python -m iceberg_agents.probes --scenario publication --work work/desktop-publication
```

Non-identity stack probes create fresh `agents_probe_*` tables and report their
names. Retain failed sandboxes. Cleanup is a later operator action confined to
reported resources, after reviewing retained evidence and references.

## Actual Book 2 health packet

The root companion environment and an existing Book 2 fleet are prerequisites.
`BOOK3_HEALTH_NAMESPACES` defaults to `bookshop`; set it to the existing fleet.

```bash
.venv/bin/python -m iceberg_agents.probes --scenario book2-health --work work/desktop-pack
```

The bridge reads through Book 2's collector without `--store`. Empty fleets fail.
Raw nulls and unknown derived counters remain unknown. Collection failure is not
a healthy result. Authored diagnostic cases are labelled separately from this packet.

## Model and semantic review

Establish Claude Code authentication on this desktop using its current supported
login flow, then choose an approved `BOOK3_MODEL`. Do not leave an API key set
accidentally when intending to use the signed-in account. API-key mode is an
explicit alternative. Record requested and observed model identities.

```bash
.venv/bin/book3 --work work/desktop-model seed
.venv/bin/python -m iceberg_agents.agent --work work/desktop-model --principal finance \
  'What is net revenue in USD cents from 2026-09-01 inclusive to 2026-10-01 exclusive?'
```

A receiver acknowledges only the actual complete envelope's run id. Review
structured evidence and free-text claims. Analytical limits are eight turns,
twenty calls per MCP process, one reported dollar and ninety seconds. Assistance
limits are two turns, half a reported dollar and sixty seconds. These settings
are limits, not measured bills or latency. Repeated evaluations incur separate
attempts; retain failures and unknown cost as well as successes.

## Before calling the workflow verified

Retain source commits, configuration without secrets, exact dependency versions,
commands, return codes and actual output. Review policy-denial controls, lineage
deletions/retries, atomic publication races and semantic judgments separately.
The four analytical trace tables are single-writer teaching projections. Local
reviewer strings and ledgers assume trusted operators. Retrieval is not an agent
tool in this draft. PostgreSQL operational-store comparisons, model retrieval
and large-scale load/retention qualification remain deployment extensions.
