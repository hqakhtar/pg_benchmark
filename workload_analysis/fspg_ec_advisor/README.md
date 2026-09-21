# FSPG EC Advisor

`fspg_ec_advisor` is a SQL-only PostgreSQL 17 extension that turns
`pg_stat_statements` and PostgreSQL statistics views into a reset-safe,
operator-facing scaling advisory.

It is installed in the fixed `fspg_ec_advisor` schema and depends on
`pg_stat_statements`. Citus telemetry is optional and discovered at capture
time.

## Install

On PostgreSQL installations that permit local extension files:

```bash
make PG_CONFIG=/path/to/pg_config install
```

Then, in a database with `pg_stat_statements` loaded through
`shared_preload_libraries`:

```sql
CREATE EXTENSION fspg_ec_advisor;
SELECT fspg_ec_advisor.capture();
```

Azure Database for PostgreSQL Flexible Server generally permits only extensions
deployed by the service. If it cannot load this package's control file, it cannot
run `CREATE EXTENSION fspg_ec_advisor`; retain the legacy runner or deploy the
advisor through an approved service-supported packaging path instead.

## Tests

The extension uses PostgreSQL's native `pg_regress` framework. Its test covers
extension creation, fingerprint scoring, JSON capture shape, capture history,
feedback/calibration, and reset-safe telemetry gating.

For the local PostgreSQL 17 source tree, this checkout is linked at:

```text
~/sources/postgres.17/contrib/fspg_ec_advisor
```

From an interactive shell in the symlinked contrib directory, run the
self-contained temporary-install test suite without any PGXS arguments:

```bash
cd ~/sources/postgres.17/contrib/fspg_ec_advisor
make check
```

When invoking through `make -C`, pass `PG_CONFIG` so the Makefile can locate
the PostgreSQL source root:

```bash
make -C ~/sources/postgres.17/contrib/fspg_ec_advisor \
  PG_CONFIG=~/sources/postgres.17/inst/bin/pg_config check
```

The Makefile includes `contrib/pg_stat_statements` in the temporary install and
uses `test/fspg_ec_advisor.conf` to preload it for regression tests. When the
local source tree contains Citus, `make check` also runs the Citus metadata
scenario with `test/fspg_ec_advisor_citus.conf`. Set `WITH_CITUS_TESTS=0` to
skip that optional scenario or `WITH_CITUS_TESTS=1` to require it explicitly.
This path uses PostgreSQL's in-tree PGXS implementation through
`contrib-global.mk`.

The matrix has three fresh-install scenarios:

- `fspg_ec_advisor`: core capture, feedback, reset-safe baseline, and reset
  GUC behavior.
- `fspg_ec_advisor_config`: dependency enforcement, unset/off/invalid/on reset
  GUC states, invalid inputs, disabled/enabled policy settings, fresh/stale
  infrastructure telemetry, event lifecycle, Azure Monitor outbox claims,
  success, retry, terminal failure, suppression, and resolution.
- `fspg_ec_advisor_citus`: optional Citus extension and metadata paths when
  Citus tests are enabled.

Run the non-Citus matrix explicitly with:

```bash
make WITH_CITUS_TESTS=0 check
```

Run the Azure Monitor dispatcher unit suite independently of PostgreSQL build
tools:

```bash
make python-test
```

The current local test matrix covers the default extension flow, 51 configured
and negative-path assertions, six Citus metadata assertions when enabled, and
eight dispatcher unit tests. It does not send a live request to Azure Monitor.

The PostgreSQL regression suite validates the durable outbox contract, not a
live Azure subscription. Test Managed Identity authentication, Logs Ingestion,
Data Collection Rules, scheduled query alerts, and action groups in an Azure
integration environment after those resources are configured.

For a standalone installed-PGXS build, force that mode explicitly:

```bash
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config install
make USE_PGXS=1 PG_CONFIG=/path/to/pg_config installcheck
```

## API

```sql
-- Store and return one advisor snapshot.
SELECT fspg_ec_advisor.capture();

-- Tune capture evidence requirements.
SELECT fspg_ec_advisor.capture(
  p_pressure_samples => 3,
  p_pressure_window_minutes => 60,
  p_min_window_seconds => 300
);

-- Reset only pg_stat_statements after saving a capture. It is opt-in.
SET fspg_ec_advisor.reset_stats = on;
SELECT fspg_ec_advisor.capture_and_reset();

-- Calling this while the GUC is off returns false and emits INFO.
RESET fspg_ec_advisor.reset_stats;
SELECT fspg_ec_advisor.reset_stats();

-- Query per-fingerprint workload classifications.
SELECT *
FROM fspg_ec_advisor.query_scores()
ORDER BY total_exec_ms DESC;

-- Fetch the most recent persisted advisor result.
SELECT fspg_ec_advisor.latest_advice();

-- Record an operator outcome and inspect calibration evidence.
SELECT fspg_ec_advisor.record_feedback(
  '<capture_key>',
  'ADD_CONNECTION_POOLING',
  'improved',
  'Waiting-session share fell after pooling.'
);

SELECT * FROM fspg_ec_advisor.recommendation_calibration;
```

The extension stores durable baselines and feedback in
`fspg_ec_advisor.capture_history` and
`fspg_ec_advisor.recommendation_feedback`. Its capture result is JSONB so
clients can evolve independently of a fixed CSV column contract.

`fspg_ec_advisor.reset_stats` is a session-level custom GUC. It defaults to
off when unset. Explicit `off` and malformed values are also treated as off and
emit `INFO`. The reset APIs only reset `pg_stat_statements`; they never reset
database, I/O, or checkpointer statistics because those counters underpin the
advisor's reset-safe resource windows.

## Automated Advisory

Run one cycle from an external scheduler or `pg_cron` when it is available:

```sql
SELECT fspg_ec_advisor.run_advisory_cycle();
```

When `pg_cron` is installed and preloaded, the extension can register a named
job directly:

```sql
SELECT fspg_ec_advisor.schedule_advisory_cycle('*/15 * * * *');
SELECT fspg_ec_advisor.unschedule_advisory_cycle(<job_id>);
```

If `pg_cron` is unavailable, these functions return `NULL` / `false` and emit
`INFO`; use an external scheduler instead. A SQL-only extension cannot schedule
itself without `pg_cron` or an external caller.

The cycle captures advisor data, evaluates the `default` policy, updates the
advisory event lifecycle, queues Azure Monitor delivery records, and resolves
stale events. A SQL-only extension cannot schedule itself, so the scheduler is
intentionally external to the extension.

Policies live in `fspg_ec_advisor.advisory_policy`. They control confidence,
capture-window requirements, connection and rate thresholds, infrastructure
thresholds, cooldowns, auto-resolution, and Azure Monitor delivery.

Infrastructure monitoring is supplied externally rather than embedded with
cloud credentials in PostgreSQL:

```sql
SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
  p_source => 'azure-monitor',
  p_cpu_pct => 92,
  p_memory_pct => 81,
  p_storage_latency_ms => 24,
  p_iops_utilization_pct => 89,
  p_slo_error_pct => 1.2
);
```

Use `acknowledge_advisory_event()` or `suppress_advisory_event()` to manage
open events. The extension emits durable delivery records through
`azure_monitor_outbox`; see [Azure Monitor delivery](docs/azure-monitor.md) for
the Managed Identity dispatcher contract and alert-rule flow.