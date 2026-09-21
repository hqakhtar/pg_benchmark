# FSPG EC Advisor SQL Decision Guide

This two-page guide explains how the SQL-only extension turns PostgreSQL and
optional Citus signals into an operator action. It separates the provisional
recommendation returned by `capture()` from the policy-controlled event created
by `run_advisory_cycle()`.

## Page 1: Evidence And Capture

### 1. Score Query Fingerprints

`query_scores()` reads top-level statements from `pg_stat_statements` for the
current database. It excludes advisor, Citus statistics, transaction-control,
and maintenance statements so its own collection work does not influence the
result.

Each fingerprint receives four heuristic scores. The scores are not a query
label; one query can contribute evidence to more than one profile. `capture()`
then weights those scores by total execution time, so the stored percentages
describe where observed execution time is concentrated.

| Profile | Main evidence | Interpretation |
| --- | --- | --- |
| OLTP | Fast, small-row, frequent DML; simple shapes; planning or lock waits | Many short transactional operations |
| OLAP | Long scans, large result sets, joins, aggregates, temp I/O, I/O waits | Scan and analytical pressure |
| HTAP | A meaningful overlap of transactional and analytical signals | Mixed operational and analytical work |
| Time series | Inserts plus time functions, time-like columns, intervals, or recent-window queries | Ingest and time-window access patterns |

Optional sources make the classification more informative: wait sampling adds
I/O, lock, and IPC fractions; Citus statistics identify cross-node work. For
Citus, a routed query with a partition key is treated as local. Other observed
Citus execution is treated as cross-node.

### 2. Catalogs And Why They Are Read

| Relation or API | Signals read | Why the decision uses it |
| --- | --- | --- |
| `pg_stat_statements` | Query ID, calls, execution and planning time, rows, block counters, and normalized query text | Scores each fingerprint and weights the workload profile by execution time. The installer resolves the extension schema dynamically. |
| `pg_stat_statements_info` | `stats_reset` | Invalidates a comparison window after a statement-statistics reset. |
| `pg_database` | Current database OID | Limits statement and database statistics to the current database and keys captures correctly. |
| `pg_settings` | Connection limits, reserved slots, and `track_io_timing` | Calculates usable connection capacity and records whether I/O timing improves coverage. |
| `pg_stat_activity` | Client backend count, active state, waits, idle-in-transaction state | Detects connection saturation and waiting-session pressure. |
| `pg_stat_database` | Cache counters, temp bytes, deadlocks, I/O time, and `stats_reset` | Produces per-database rates and rejects windows after a database-statistics reset. |
| `pg_stat_io` | Client-backend reads, writes, and `stats_reset` | Adds reset-safe client I/O rates. |
| `pg_stat_checkpointer` | Timed/requested checkpoints and `stats_reset` | Detects checkpoint-request pressure and reset boundaries. |
| `query_store.pgms_wait_sampling_view` when installed | I/O, lock, and IPC wait samples by query ID | Adds optional wait fractions to fingerprint scoring; absence lowers coverage but does not fail capture. |
| `pg_extension` plus `to_regclass()` | Whether Citus and its optional relations exist | Keeps the extension usable on PostgreSQL without Citus. |
| `pg_catalog.citus_stat_statements` when available | Executor, partition key, and query ID | Marks execution as local router work or cross-node work for locality and OLAP evidence. |
| `pg_catalog.pg_dist_node` and `pg_catalog.citus_shards` when available | Active primary workers and shard bytes per worker | Calculates shard-byte fairness before recommending more Citus workers. |

The Citus relations are optional. The SQL checks their existence before reading
them; missing Citus telemetry becomes a coverage limitation, not a query error.

### 3. Create A Reset-Safe Window

`resource_snapshot()` gathers connection occupancy and wait state from
`pg_stat_activity`, counters from `pg_stat_database`, client I/O from
`pg_stat_io`, and checkpoint counters from `pg_stat_checkpointer`. `capture()`
persists cumulative values, then compares them with the prior capture for the
same server and database.

A window is valid only when it has a prior capture, meets the minimum duration,
has no statistics reset, and no counter regresses. The default minimum is 300
seconds. Only a valid window produces rates such as temp bytes per second,
deadlocks per hour, client I/O per second, and checkpoint requests per hour.

The capture's connection-pressure signal is one of:

- `SATURATED_CONNECTIONS`
- `HIGH_CONNECTION_PRESSURE`
- `HIGH_CONNECTION_UTILIZATION`
- `NO_CONNECTION_PRESSURE_SIGNAL`

For its provisional recommendation, `capture()` requires the current valid
window plus three pressure observations in the last 60 minutes by default.
This avoids treating one short spike or a reset as capacity evidence.

### 4. Grade Evidence And Workload Fit

Telemetry coverage starts at 30 percent. It gains 30 points for a valid window,
10 for `track_io_timing`, 10 for optional wait sampling, and 20 when Citus is
not installed or both Citus fairness and statement telemetry are available.
Coverage and capture history determine confidence:

| Confidence | Requirement |
| --- | --- |
| High | Valid window, required capture count, and at least 80 percent coverage |
| Medium | Valid window and at least 60 percent coverage |
| Low | Anything else |

Workload fit is evaluated in a safety-first order. If active Citus worker shard
bytes have fairness below `0.85`, the result is `REBALANCE_FIRST`. If cross-node
execution is at least 40 percent or routability is below `0.60`, it is
`OPTIMIZE_LOCALITY_FIRST`. Only after those blockers are clear does the advisor
consider horizontal scale: OLAP plus HTAP at least 40 percent, or OLTP at least
50 percent, becomes `SCALE_OUT` on Citus or `CITUS_MIGRATION_CANDIDATE` without
Citus.

`capture()` stores all source values, rates, blockers, confidence, workload fit,
expected benefit, and verification step in `capture_history.payload`. Its action
is an evidence summary, not yet a notification decision.

## Page 2: Policy And Event Decisions

`run_advisory_cycle()` calls `capture()` using the selected policy's sample
count, pressure window, and minimum window. It then calls `evaluate_capture()`.
This is the point where the extension decides whether an operator-facing event
should exist.

### 5. The Default Policy

`advisory_policy` holds mutable thresholds, so tuning does not require changing
the extension SQL. The `default` row starts with these values:

| Concern | Default policy |
| --- | --- |
| Confidence and persistence | At least `MEDIUM`; 3 samples in 60 minutes; 300-second minimum window |
| Connection pressure | 85 percent utilization and 20 percent waiting active sessions |
| Database rates | 16 MiB/s temp data, 1 deadlock/hour, 1,000 client reads/s, 1,000 client writes/s, 6 checkpoint requests/hour |
| Infrastructure | 85 percent CPU, memory, or IOPS; 20 ms storage latency; queue depth 5; 1 percent SLO errors; optional application-latency limit |
| External telemetry | Must be no more than 15 minutes old |
| Event behavior | 30-minute notification cooldown; auto-resolve after 2 hours; Azure Monitor delivery enabled |

Fresh infrastructure telemetry is stored separately by
`ingest_infrastructure_telemetry()`. The evaluator does not silently treat a
missing or stale infrastructure sample as proof of infrastructure pressure.

### 6. Apply The Policy Gates

`evaluate_capture()` proceeds in this order:

1. Reject a disabled or unknown policy.
2. Reject a capture whose confidence is below `minimum_confidence`.
3. Calculate connection pressure, rate pressure, and fresh infrastructure
   pressure against the policy thresholds.
4. Require the current capture to have a valid window and enough valid captures
   in the pressure window.
5. Require either enough database-pressure captures or enough pressured,
   fresh infrastructure samples.
6. Select one action using the following precedence.

| First matching condition | Policy action |
| --- | --- |
| Capture says rebalance or locality must come first | `REBALANCE_FIRST` or `OPTIMIZE_LOCALITY_FIRST` |
| Pressure is not sustained | No event |
| Connection pressure and at least 50 percent OLTP | `ADD_CONNECTION_POOLING` |
| Citus scale-out fit | `ADD_CITUS_WORKERS_CANDIDATE` |
| Non-Citus scale-out fit | `CITUS_MIGRATION_CANDIDATE` |
| Connection, rate, or infrastructure pressure | `SCALE_UP_CANDIDATE` |
| Remaining sustained case | `TUNE_QUERY_OR_INDEX` |

This means a capture may say `MONITOR` while policy evaluation emits no event;
it also means an event is not created merely because one counter crosses a
threshold. Confidence, a reset-safe time window, and sustained evidence are
all required first.

### 7. Manage Events And Delivery

Active events are deduplicated by server, database, policy, and action. A later
matching capture updates the same event and increments its occurrence count.
An acknowledged or suppressed event remains the same event; a suppression
expires automatically. A changed or absent action resolves older active events.

When Azure Monitor delivery is enabled and the notification cooldown has
elapsed, the event is written to the durable `azure_monitor_outbox`. The Python
dispatcher, not PostgreSQL, delivers that record. A failed delivery is retried
with backoff; a resolved event also receives an outbox record.

To tune the default policy, update the row rather than editing the installer:

```sql
UPDATE fspg_ec_advisor.advisory_policy
SET
    minimum_pressure_samples = 4,
    pressure_window = interval '90 minutes',
    notification_cooldown = interval '45 minutes',
    updated_at = clock_timestamp()
WHERE policy_name = 'default';
```

Use `latest_advice()` to inspect the most recent evidence payload and
`recommendation_calibration` to compare recorded operator outcomes by action
and confidence. Tune thresholds from that feedback, not from a single event.