# FSPG EC Advisor

**FSPG EC Advisor** is the PostgreSQL / Citus workload and capacity advisor in
this directory. Its canonical delivery is the SQL-only PostgreSQL extension in
[workload_analysis/fspg_ec_advisor](fspg_ec_advisor), installed as
`fspg_ec_advisor`:

```sql
CREATE EXTENSION fspg_ec_advisor;
SELECT fspg_ec_advisor.capture();
```

The extension replaces psql orchestration with a database-native capture API,
durable advisor history, and feedback calibration. The existing
`workload_score_pg_stat_statements.sql` file remains below as the **legacy psql
compatibility runner** until the Phase 3 dashboard moves to a direct PostgreSQL
connection.

---

## Legacy Runner

A read-only profiler that inspects a PostgreSQL / Citus database and reports
what *kind* of workload it is running: **OLTP**, **OLAP**, **HTAP**, and
**TIME_SERIES**. It reads only statistics views (`pg_stat_statements`, Citus's
`citus_stat_statements` / `citus_shards`, and, when available, Azure's
`pgms_wait_sampling`), classifies every query fingerprint, and aggregates the
results into workload-level percentages.

The script keeps its per-run calculations in temporary tables and commits at the
end. PostgreSQL requires temporary relations to live in `pg_temp`, so the script
gives them generated `citus_<server>_<database>_<timestamp>_*` names. Phase 1
also creates a small durable `citus_advisor` schema for capture baselines,
operator feedback, and calibration; this state is required for reset-safe rate
calculation and sustained-pressure decisions.

---

## What it does

1. **Checks prerequisites** — aborts with a clear error if
   `pg_stat_statements` is not installed.
2. **Optionally folds in wait data** — if Azure's
   `query_store.pgms_wait_sampling_view` is reachable, it rolls up per-query
   wait events (IO / Lock / IPC / Client). If not, it degrades gracefully:
   all wait signals become zero and a `NOTICE` is raised.
3. **Optionally detects Citus cross-node queries** — if `citus_stat_statements`
   is present, each fingerprint is flagged as single-node (a `router` execution
   with a partition key) or cross-node (anything that fans out). The cross-node
   flag feeds the OLAP score. If Citus is absent it degrades gracefully (all
   queries treated as single-node) and a `NOTICE` is raised.
4. **Builds a generated temporary feature table** per query fingerprint:
   - `base` — clean raw counters from `pg_stat_statements`.
   - `features` — derived ratios and yes/no flags parsed from the query text.
   - `scored` — four weighted scores (OLTP / OLAP / TIME_SERIES / HTAP).
   - `weighted` — attaches importance weights (calls, exec time, I/O blocks).
5. **Optionally measures Citus data distribution** — if `citus_shards` is
  present, it computes a per-table and cluster-wide shard-byte *fairness*
  score, including active workers that currently hold zero shards.
6. **Scores query routability** — how local / single-node the workload is.
7. **Captures PostgreSQL 17 resource pressure** — connection utilization and
  waiting sessions are evaluated at collection time; database, I/O, and
  checkpoint counters are exported with their reset timestamps for trend and
  rate calculation.
8. **Builds a Phase 1 operator assessment** — compares reset-compatible
  captures, requires sustained pressure before capacity candidates, and emits
  confidence, telemetry gaps, evidence, expected benefit, and a verification
  step.
9. **Emits up to nine result sets** (see below) and appends a one-row workload
  summary to a CSV for Grafana.

### Run it

By default the script prints **all result sets (1–9)** to psql. Add
`-v verbose=off` to suppress the detailed intermediate sets and emit only the
final consolidated summary (result set 9). Either way result set 9 is also
appended to the Grafana CSV.

```bash
# Full detail (result sets 1–9) — the default
psql -X -v ON_ERROR_STOP=1 -d <database> \
  -f workload_analysis/workload_score_pg_stat_statements.sql

# Consolidated summary only (result set 9)
psql -X -v ON_ERROR_STOP=1 -v verbose=off -d <database> \
  -f workload_analysis/workload_score_pg_stat_statements.sql

# Custom CSV path for the Grafana export (default: workload_score.csv)
psql -X -v ON_ERROR_STOP=1 -v csv=/abs/path/workload_score.csv -d <database> \
  -f workload_analysis/workload_score_pg_stat_statements.sql

# Tune Phase 1 capture requirements for the local collection cadence.
psql -X -v ON_ERROR_STOP=1 \
  -v advisor_pressure_samples=3 \
  -v advisor_pressure_window_minutes=60 \
  -v advisor_min_window_seconds=300 \
  -d <database> -f workload_analysis/workload_score_pg_stat_statements.sql
```

> Tip: For meaningful output, run it against a database that has handled a real
> workload (e.g. a TPCC-loaded benchmark DB). An idle database produces no
> useful classification.

---

## Output

By default **all result sets (1–9)** are printed. Set `-v verbose=off` to emit
only result set 9 (the consolidated summary). Result sets 5–6 require Citus;
result sets 7–8 also work on plain PostgreSQL, where execution is local by
definition. Result set 9 is always printed and always written to the Grafana
CSV.

1. **Per-fingerprint scores** (hidden with `verbose=off`)

  One row per query with its raw scores and statistics. This is the drill-down
  detail.
2. **Aggregate percentages** (hidden with `verbose=off`)

  Percent OLTP, OLAP, HTAP, and TIME_SERIES, computed by calls, execution
  time, and I/O blocks.
3. **Planning- vs execution-bound** (hidden with `verbose=off`)

  How much of the workload is dominated by query *planning* versus
  *execution*, which is relevant for Citus. Planning fields are populated only
  when `pg_stat_statements.track_planning` is on.
4. **Wait-event summary** (hidden with `verbose=off`)

  IO, Lock, and IPC wait percentages. These are zero unless
  `pgms_wait_sampling` was reachable.
5. **Per-table data distribution fairness** (hidden with `verbose=off`)

  Each distributed table's node count, total bytes, and shard-byte fairness
  score. This result is available only with Citus.
6. **Workload mix and fairness** (hidden with `verbose=off`)

  Execution-time-weighted workload percentages and the cluster-wide fairness
  score.
7. **Per-query routability** (hidden with `verbose=off`)

  Whether each top-level query is cross-node, with the worst routability
  scores first.
8. **Routability summary** (hidden with `verbose=off`)

  The execution-time-weighted share that is ideal-local or cross-node, plus the
  execution-time-weighted mean routability score.
9. **Consolidated summary** (always shown)

  The headline workload mix, data-distribution fairness, routability,
  dominant workload, scale recommendation, and resource-pressure snapshot. It
  also includes the Phase 1 `recommended_action`, confidence, reset-safe rate
  evidence, telemetry blockers, expected benefit, and verification step. It is
  appended to the Grafana CSV.

---

## Default values it uses

All thresholds and weights below are the script's built-in defaults. They are
intentionally simple and meant to be tuned for your environment.

### What gets classified (filters)

Only queries that pass **all** of these are scored:

- In the **current database** only (`dbid` match).
  *Why:* `pg_stat_statements` is cluster-wide; mixing queries from other
  databases would pollute the profile with workloads you are not analyzing.
- **Top-level** statements only (`s.toplevel`).
  *Why:* nested statements (inside functions/procedures) are already reflected
  in their top-level caller. Counting both would double-count the same work.
- **Not** the script's own scaffolding (`_pss_wait_by_query`,
  `_pss_workload_features`, `pgms_wait_sampling_view`).
  *Why:* the classifier must not profile itself, or it would report its own
  temp-table and view queries as part of your workload.
- **Not** transaction-control / session / DDL / utility commands. Excluded
  leading keywords: `begin, start, commit, end, rollback, savepoint, release,
  prepare, deallocate, set, reset, show, discard, lock, listen, unlisten,
  notify, create, drop, alter, truncate, vacuum, analyze, analyse, reindex,
  cluster, refresh, grant, revoke, comment, security, do, call, explain, copy`.
  *Why:* these are administrative/housekeeping commands, not application data
  access. They say nothing about whether the workload is OLTP or OLAP, so
  including them would dilute the signal with noise.

### Wait-event bucketing (`pgms_wait_sampling`)

PostgreSQL reports dozens of fine-grained wait events. The script groups them
into four coarse buckets so each can act as a workload signal.

- **IO** (`IO`)

  Time spent reading or writing data files points to large scans, an **OLAP**
  trait.
- **Lock** (`Lock`, `LWLock`, and `BufferPin`)

  Contention on rows or buffers points to many concurrent short writes, an
  **OLTP** trait.
- **IPC** (`IPC`)

  Waiting on other processes points to cross-node coordination, the key
  **distributed/HTAP (Citus)** signal.
- **Client** (`Client` and `Activity`)

  Waiting on the application or network, or simply being idle, is not the
  database's doing. It is tracked but not used to classify.

### Derived feature thresholds

- **Statement type flags**

  `is_select`, `is_insert`, `is_update`, and `is_delete` match the query's
  first keyword. The verb is the most basic split: writes lean OLTP, while
  reads can be either.
- **`has_join`**

  Matches `join`. Joining tables is a hallmark of analytical queries that
  combine datasets.
- **`has_group_by`**

  Matches `group by`. Aggregation over many rows is core OLAP behavior.
- **`has_distinct`**

  Matches `distinct`. De-duplication implies scanning or sorting large result
  sets, an OLAP trait.
- **`has_order_by`**

  Matches `order by`. Sorting hints at reporting or ranking rather than point
  lookups.
- **`has_having`**

  Matches `having`. Filtering *after* aggregation only appears in analytical
  queries.
- **`has_window`**

  Matches `over (`. Window functions such as running totals and ranks are
  analytical constructs.
- **`has_limit`**

  Matches `limit`. Capping rows is typical of paginated application reads
  (OLTP) and "top-N" queries.
- **`has_time_function`**

  Matches `date_trunc`, `time_bucket`, `extract(`, `now()`,
  `current_timestamp`, or `current_date`. Bucketing or filtering by time is
  the defining trait of time-series work.
- **`has_time_column_hint`**

  Matches `created_at`, `updated_at`, `event_time`, `event_ts`, `timestamp`,
  `ts`, or `time`. Referencing time columns suggests append-mostly,
  time-ordered data.
- **`has_interval`**

  Matches `interval`. Interval math, such as "last 7 days", is a strong
  time-series indicator.
- **`recent_window_pattern`**

  Matches `order by ... desc` together with `limit`. "Newest N rows" is the
  classic time-series access pattern.

### OLTP score weights

OLTP = many small, fast, frequently repeated transactions. Each rule rewards a
trait of that profile; bigger weights = stronger evidence.

- **2.0, `mean_exec_ms < 20`**

  Very fast queries are almost always point lookups or updates, making this
  the strongest single OLTP signal.
- **1.5, `rows_per_call < 100`**

  Touching few rows per call means precise, indexed access rather than bulk
  scanning.
- **1.2, insert/update/delete**

  Transactional workloads are write-heavy; pure reads are more ambiguous.
- **1.0, no group by / distinct / window / having**

  The absence of aggregation rules out analytical intent.
- **0.8, `temp_io_fraction < 0.02` (or null)**

  Little or no temp spill means the query fits in memory and has a small
  working set.
- **0.8, `calls >= 100`**

  High call counts indicate a hot, repeated transaction path.
- **0.8, `plan_fraction > 0.15`**

  When planning is a large share of total time, execution is tiny, indicating
  a trivial query.
- **0.4, `has_limit`**

  `LIMIT` is common in paginated application reads, making it a weak
  supporting signal.
- **0.6, `lock_wait_fraction > 0.30`**

  High lock contention comes from many concurrent short writes competing for
  rows.

### OLAP score weights

OLAP = fewer, heavier, analytical queries that scan and aggregate lots of data.
The rules mirror the OLTP ones but in the opposite direction.

- **2.0, `mean_exec_ms > 200`**

  Slow queries usually mean large scans or aggregations, making this the
  strongest OLAP signal.
- **1.5, `rows_per_call > 1000`**

  Processing many rows per call is the essence of analytical work.
- **1.2, group by / distinct / window / having**

  These are explicit aggregation and analytics constructs.
- **1.0, `has_join`**

  Combining multiple large tables is typical of reporting queries.
- **1.0, `temp_io_fraction > 0.05`**

  Spilling to temp disk means the working set exceeds memory.
- **0.8, `shared_blks_per_call > 1000`**

  Reading many buffers per call indicates wide scans rather than point
  lookups.
- **0.8, `plan_fraction < 0.05`**

  When planning is negligible compared with execution, the time goes into
  processing data.
- **0.4, `is_select`**

  Analytics are read-only. This is a weak supporting signal because reads can
  also be OLTP.
- **0.6, `io_wait_fraction > 0.30`**

  Heavy disk-read waiting confirms large scans hitting storage.
- **1.0, `is_cross_node` (Citus)**

  A query that fans out across worker nodes instead of using a single-node
  `router` execution is doing distributed scatter/gather, an analytical OLAP
  trait.

### TIME_SERIES score weights

Time-series = append-mostly, time-ordered data (metrics, events, logs). It is
treated as an *orthogonal* pattern: a query can be time-series **and**
OLTP/OLAP.

- **2.0, `is_insert`**

  Time-series systems are dominated by high-rate appends of new events.
- **1.4, time function or time column hint**

  Bucketing or filtering by time is the defining operation.
- **1.0, `has_interval`**

  Relative time math, such as "last N minutes", is specific to time-series
  queries.
- **1.0, `recent_window_pattern`**

  "Most recent N rows" is the canonical time-series read.
- **0.8, `rows_per_call BETWEEN 1 AND 100000`**

  Time-windowed reads return a bounded slice rather than one row or the whole
  table.
- **0.6, group by and a time function or column**

  Grouping *by time*, such as per-minute rollups, is a classic downsampling
  query.

### HTAP score weights

HTAP = a hybrid workload mixing transactional and analytical traits. Its core
term is `2.0 × LEAST(oltp-core, olap-core)`, where each core is normalized by
dividing by `4.7` (the sum of the three core sub-weights).

*Why `LEAST`:* taking the **minimum** of the OLTP and OLAP cores means HTAP only
scores high when a query is *both* somewhat-transactional **and**
somewhat-analytical. If it is strongly one and weakly the other, the minimum is
small and HTAP stays low — exactly the "mixed" definition we want.

- **2.0, `LEAST(oltp_core, olap_core)` (overlap term)**

  Rewards genuine overlap of both profiles rather than dominance by one.
- **0.8, `plan_fraction BETWEEN 0.05 AND 0.30`**

  A balanced plan/execution split sits between trivial OLTP and heavy OLAP.
- **0.8, `calls >= 10 AND mean_exec_ms >= 5`**

  Repeated and non-trivial queries are too frequent for pure OLAP and too
  heavy for pure OLTP.
- **0.6, join / group by / write**

  Mixing analytical operators with write activity is the hallmark of HTAP.
- **0.5, `ipc_wait_fraction > 0.20`**

  Cross-node coordination in Citus typically arises from mixed distributed
  workloads.

### Aggregate weighting models (Result set 2)

The same per-query scores are aggregated three ways because "what is the
workload?" has three valid answers depending on what you care about.

- **`calls`** (number of calls)

  Answers what the workload looks like by query *volume*, favoring frequent
  OLTP.
- **`execution_time_ms`** (total execution time)

  Answers where the server *spends its time*, surfacing expensive OLAP even
  when it is rare.
- **`io_blocks`** (shared, temp, and local blocks)

  Answers what drives *I/O and resource* usage, highlighting data-heavy
  queries.

### Planning vs execution thresholds (Result set 3)

This summary matters for **Citus**, where distributed query *planning* can
become a bottleneck independent of how much data is actually processed. It is
only evaluated while `pg_stat_statements.track_planning` is enabled.

- **Planning-bound: `plan_fraction >= 0.30`**

  A third or more of the time spent planning signals planner overhead, a known
  distributed-query cost worth tuning.
- **Execution-bound: `plan_fraction <= 0.05`**

  Almost all time in execution means the work is real data processing rather
  than planning.
- **OLTP-like but planning-heavy**

  Rule: `mean_exec_ms < 20 AND plan_fraction >= 0.30`. Fast queries dominated
  by planning are prime candidates for prepared statements or plan caching.
- **OLAP-like and execution-heavy**

  Rule: `mean_exec_ms > 200` with group by, window, distinct, or having.
  This confirms heavy analytical queries whose cost is genuine execution, not
  planning.

### Citus cross-node detection (`citus_stat_statements`)

When Citus is present, each fingerprint is classified as single-node or
cross-node from `citus_stat_statements`:

- **Single-node**

  Rule: `executor = 'router'` and `partition_key IS NOT NULL`. The query is
  routed to one worker by its distribution key, the cheap and scalable path.
- **Cross-node**

  Rule: anything else. The query fans out across workers (scatter/gather),
  which feeds the OLAP score and the routability penalty.

### Data distribution fairness (Result sets 5 & 6)

On a Citus cluster the script measures how evenly each distributed table's shard
bytes are spread across worker nodes, using a coefficient-of-variation score:

```
fairness = clamp(0, 1 − stddev_pop(node_bytes) / mean(node_bytes))
```

- **`1.00`**: Every node holds the same number of shard bytes and is perfectly
  balanced.
- **Toward `0`**: Bytes are concentrated on a few nodes, indicating skewed
  distribution.

It is reported per distributed table and as a `(cluster overall)` row. Active
primary workers are included even when they hold zero shard bytes, so a newly
added but empty worker cannot make an imbalanced cluster appear perfectly
balanced.

> *Note:* this measures **byte** skew, not **load** skew — a table can be evenly
> sized yet still have a hot shard. Off Citus the result sets are empty and a
> `NOTICE` is raised.

### Query routability / locality (Result sets 7 & 8)

Every top-level application statement gets a routability score in `(0, 1]` that
starts at `1.00` and is penalized when it runs cross-node:

- **×0.5, runs cross-node** (from the Citus rollup)

  Scatter/gather across workers is costlier than a single-node route.

The lowest score is `0.50`. Nested statements are excluded so their execution
time is not counted both in their parent and their own fingerprint. The summary
reports execution-time-weighted ideal-local and cross-node shares plus the
execution-time-weighted mean routability score.

### Consolidated summary & Grafana CSV (Result set 9)

The final row combines the execution-time-weighted workload mix, the
cluster-wide data-distribution fairness, and the routability headline metrics.
It is always printed (even without `verbose`) and is appended as a timestamped
row to a CSV (`workload_score.csv` by default, override with
`-v csv=/abs/path.csv`) so a Grafana CSV/Infinity datasource can chart the
workload over time.

For a plain PostgreSQL server, a qualifying `SCALE_OUT` recommendation means
moving the workload to Citus / Azure Elastic Clusters for horizontal scale-out;
on Citus, it means adding worker capacity to the existing cluster.

### Resource pressure snapshot (Result set 9)

The final row exports these PostgreSQL 17 signals alongside the workload
recommendation:

- **Connection pressure**: client backend count, usable connection capacity,
  active/waiting sessions, idle-in-transaction sessions, and a conservative
  `resource_pressure_status`. This is the only immediate saturation signal.
- **Current-database counters**: cache-hit ratio, block read/write timing, temp
  bytes, and deadlocks from `pg_stat_database`.
- **Server-wide counters**: client-backend reads/writes from `pg_stat_io`, plus
  requested/timed checkpoints and checkpoint timing from `pg_stat_checkpointer`.

I/O, temp, deadlock, and checkpoint values are cumulative since their associated
`*_stats_reset` timestamp. Compare consecutive CSV captures and discard an
interval crossing a reset before using them as rates. `track_io_timing` marks
whether database I/O timing is available. PostgreSQL statistics do not expose
host CPU, memory, storage queue depth, or cloud IOPS saturation, so this snapshot
must complement rather than replace infrastructure monitoring.

### Phase 1 Operator Assessment (Result set 9)

The advisor saves each final capture in `citus_advisor.capture_history`, scoped
by PostgreSQL server and database. A rate window is valid only when the previous
capture has matching `pg_stat_statements`, database, I/O, and checkpointer reset
timestamps and meets `advisor_min_window_seconds`.

Capacity-oriented actions require `advisor_pressure_samples` pressure captures
inside `advisor_pressure_window_minutes`; the defaults are three captures within
60 minutes. Until that evidence exists, workload-fit outcomes are gated to
`MONITOR` or `REVIEW_TELEMETRY` rather than capacity actions.

The authoritative field is `recommended_action`; `workload_fit_recommendation`
retains the prior workload-only result for comparison. Phase 1 action types are:

- `REVIEW_TELEMETRY` and `MONITOR`
- `TUNE_QUERY_OR_INDEX` and `ADD_CONNECTION_POOLING`
- `SCALE_UP_CANDIDATE`
- `CITUS_MIGRATION_CANDIDATE` and `ADD_CITUS_WORKERS_CANDIDATE`
- `REBALANCE_FIRST` and `OPTIMIZE_LOCALITY_FIRST`

Record an operator outcome after acting on a capture:

```sql
SELECT citus_advisor.record_feedback(
  '<capture_key>',
  'ADD_CONNECTION_POOLING',
  'improved',
  'Waiting sessions fell after pooling was enabled.'
);

SELECT * FROM citus_advisor.recommendation_calibration;
```

The calibration view groups observed outcomes by action and confidence. It is
evidence for threshold tuning, not automatic threshold modification.

> The CSV export uses `COPY ... TO PROGRAM`, which runs on the **server** and
> needs superuser or the `pg_execute_server_program` role. On failure the export
> is skipped with a `NOTICE` and the rest of the script still succeeds. A
> relative path resolves against the server's `data_directory`, so pass an
> absolute path for a predictable location.

---

## Notes & limitations

- **`track_planning`**: plan-based signals (`plan_fraction`, planning-bound
  metrics) are only considered when `pg_stat_statements.track_planning` is on.
  When it is off, planning fields remain null and do not affect scoring.
- **Statistics visibility**: run as a role able to inspect all database
  activity and statistics for complete resource-pressure data; otherwise session
  counts and wait states can be incomplete.
- **Advisor-state privileges**: the first run requires permission to create and
  write the `citus_advisor` schema. Capture history intentionally persists; use
  normal database retention and access controls for that operational data.
- **Wait data is per-node**: on a multi-node Citus / Elastic Cluster, the
  wait-sampling view reflects only the node you connect to. Gather waits from
  each node for the full picture.
- **Azure cross-database limitation**: on Flexible Server the wait-sampling
  data lives in the `azure_sys` database. PostgreSQL cannot join across
  databases, so wait signals only contribute when the view is visible from the
  database the script runs in.
- **Thresholds are tunable**: the numbers above are starting points, not
  universal truths. Adjust them to match your hardware and workload.
