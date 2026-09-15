-- workload_score_pg_stat_statements.sql
--
-- Purpose
--   Build a pg_stat_statements-driven classifier that scores each fingerprint
--   and then aggregates those scores into workload-level percentages.
--
-- Output
--   1) Per-query fingerprint scoring table
--   2) Aggregate percentages weighted by calls, execution time, and buffer/temp I/O
--   3) A planning-bound vs execution-bound summary useful for Citus analysis
--   4) A wait-event category summary (only when pgms_wait_sampling is reachable)
--
-- Requirements
--   * pg_stat_statements must be installed.
--   * Prefer PostgreSQL / pg_stat_statements versions exposing planning columns
--     (total_plan_time / mean_plan_time). If not available, the script falls back.
--
-- Optional: pgms_wait_sampling (Azure Database for PostgreSQL Flexible Server)
--   * When the Query Store wait-sampling view query_store.pgms_wait_sampling_view
--     is reachable in the current database, its per-query wait events are folded
--     in as additional signals (IO waits -> OLAP, Lock/LWLock waits -> OLTP
--     contention, IPC waits -> distributed/HTAP).
--   * The wait view is matched to pg_stat_statements on queryid and filtered to
--     the current database OID.
--   * NOTE: On Azure Flexible Server the wait-sampling data lives in the
--     azure_sys database (schema query_store). Because PostgreSQL cannot join
--     across databases, the wait signals only contribute when the view is
--     visible from the database this script runs in. When it is not reachable,
--     the script degrades gracefully: all wait signals are zero and a NOTICE is
--     raised. The pg_stat_statements scoring is unaffected.
--   * pg_stat_kcache and auto_explain are intentionally not used. auto_explain
--     only writes plans to the server log and exposes no SQL-queryable view.
--
-- Notes
--   * Only top-level DML statements are classified. The script filters out its
--     own scaffolding (temp tables, DO blocks, result-set queries) and all
--     transaction-control / session / DDL / utility commands so it does not
--     profile itself or count administrative noise as workload.
--   * TIME_SERIES is treated as an orthogonal pattern; percentages are normalized
--     across OLTP / OLAP / HTAP / TIME_SERIES for convenience.
--   * Scoring thresholds are intentionally simple and tunable.
--
-- Run with:
--   psql -f workload_score_pg_stat_statements.sql

\pset tuples_only off
\pset pager off

-- Output verbosity. By default all result sets (1-9) are emitted to psql. To
-- emit only the final consolidated score (result set 9), set the psql session
-- variable 'verbose' to off, e.g.:
--   \set verbose off         (interactive / .psqlrc)
--   psql -v verbose=off ...   (command line)
-- Either way the consolidated score (result set 9) is still printed AND appended
-- to the Grafana CSV.
\if :{?verbose}
\else
\set verbose on
\endif

-- CSV export path for Grafana. The final consolidated score (result set 9) is
-- appended here as a timestamped row on every run (the header is written once),
-- so Grafana's CSV/Infinity datasource can chart the workload mix over time.
-- Override with  -v csv=/path/to/file.csv  or  \set csv '/abs/path.csv'.
\if :{?csv}
\else
\set csv 'workload_score.csv'
\endif

BEGIN;

DO $plpgsql$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'pg_stat_statements'
    ) THEN
        RAISE EXCEPTION 'pg_stat_statements is not installed in database %', current_database();
    END IF;
END
$plpgsql$;

-- Optional pgms_wait_sampling integration.
-- Build a per-query wait-event rollup keyed by queryid. The table is always
-- created so downstream LEFT JOINs work; it stays empty (all-zero signals) when
-- the Query Store wait-sampling view is not reachable in this database.
SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_wait_by_query;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_wait_by_query (
    queryid             bigint,
    wait_samples        numeric,
    io_wait_samples     numeric,
    lock_wait_samples   numeric,
    ipc_wait_samples    numeric,
    client_wait_samples numeric,
    io_wait_fraction    numeric,
    lock_wait_fraction  numeric,
    ipc_wait_fraction   numeric
);

DO $plpgsql$
DECLARE
    v_relid oid := to_regclass('query_store.pgms_wait_sampling_view');
BEGIN
    IF v_relid IS NULL THEN
        RAISE NOTICE 'pgms_wait_sampling not reachable in database % (query_store.pgms_wait_sampling_view absent); wait signals disabled. On Azure Flexible Server this view lives in the azure_sys database.', current_database();
        RETURN;
    END IF;

    INSERT INTO _pss_wait_by_query
    WITH w AS (
        SELECT
            query_id::bigint                                                       AS queryid,
            event_type::text                                                       AS event_type,
            calls::numeric                                                         AS samples
        FROM query_store.pgms_wait_sampling_view
        WHERE db_id = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND query_id IS NOT NULL
    ),
    agg AS (
        SELECT
            queryid,
            sum(samples)                                                                       AS wait_samples,
            sum(samples) FILTER (WHERE event_type = 'IO')                                      AS io_wait_samples,
            sum(samples) FILTER (WHERE event_type IN ('Lock', 'LWLock', 'BufferPin'))          AS lock_wait_samples,
            sum(samples) FILTER (WHERE event_type = 'IPC')                                     AS ipc_wait_samples,
            sum(samples) FILTER (WHERE event_type IN ('Client', 'Activity'))                   AS client_wait_samples
        FROM w
        GROUP BY queryid
    )
    SELECT
        queryid,
        COALESCE(wait_samples, 0),
        COALESCE(io_wait_samples, 0),
        COALESCE(lock_wait_samples, 0),
        COALESCE(ipc_wait_samples, 0),
        COALESCE(client_wait_samples, 0),
        COALESCE(io_wait_samples, 0)   / NULLIF(wait_samples, 0),
        COALESCE(lock_wait_samples, 0) / NULLIF(wait_samples, 0),
        COALESCE(ipc_wait_samples, 0)  / NULLIF(wait_samples, 0)
    FROM agg;

    RAISE NOTICE 'pgms_wait_sampling integrated: % queries with wait samples in database %.',
        (SELECT count(*) FROM _pss_wait_by_query), current_database();
END
$plpgsql$;

-- Optional Citus cross-node detection (built before the classifier so the
-- cross-node signal can feed the OLAP score). citus_stat_statements exposes,
-- per fingerprint, the executor used and the routing key. A 'router' execution
-- with a partition key runs on a single node; anything else fans out across
-- nodes - a distributed scatter/gather that is treated as an OLAP signal. The
-- table is always created so downstream joins work; it stays empty when Citus
-- is absent (citus_stat_statements absent) and a NOTICE is raised, in which
-- case cross-node defaults to 0 (single node).
SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_query_crossnode;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_query_crossnode (
    queryid        bigint,
    is_cross_node  integer
);

DO $plpgsql$
DECLARE
    v_relid oid := to_regclass('citus_stat_statements');
BEGIN
    IF v_relid IS NULL THEN
        RAISE NOTICE 'Citus not detected (citus_stat_statements absent); cross-node detection disabled.';
        RETURN;
    END IF;

    INSERT INTO _pss_query_crossnode
    SELECT
        queryid,
        max(CASE WHEN executor = 'router' AND partition_key IS NOT NULL THEN 0 ELSE 1 END) AS is_cross_node
    FROM citus_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND queryid IS NOT NULL
    GROUP BY queryid;

    RAISE NOTICE 'Citus cross-node detection: % fingerprint(s) classified.',
        (SELECT count(*) FROM _pss_query_crossnode);
END
$plpgsql$;

SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_workload_features;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_workload_features AS
WITH base AS (
    SELECT
        s.queryid,
        s.userid,
        s.dbid,
        s.toplevel,
        s.calls::numeric                                                                   AS calls,
        s.total_exec_time::numeric                                                         AS total_exec_ms,
        COALESCE(s.mean_exec_time::numeric,
                 s.total_exec_time::numeric / NULLIF(s.calls, 0), 0)                       AS mean_exec_ms,
        COALESCE(s.rows::numeric, 0)                                                       AS rows,
        COALESCE(s.rows::numeric / NULLIF(s.calls, 0), 0)                                  AS rows_per_call,
        COALESCE((s.shared_blks_hit + s.shared_blks_read)::numeric, 0)                     AS shared_blks_total,
        COALESCE((s.shared_blks_dirtied + s.shared_blks_written)::numeric, 0)              AS shared_blks_write_total,
        COALESCE((s.temp_blks_read + s.temp_blks_written)::numeric, 0)                     AS temp_blks_total,
        COALESCE((s.local_blks_hit + s.local_blks_read
                + s.local_blks_dirtied + s.local_blks_written)::numeric, 0)                AS local_blks_total,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = 'pg_stat_statements'
                  AND column_name  = 'total_plan_time'
            ) THEN s.total_plan_time::numeric
            ELSE 0::numeric
        END                                                                                AS total_plan_ms,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = 'pg_stat_statements'
                  AND column_name  = 'mean_plan_time'
            ) THEN s.mean_plan_time::numeric
            ELSE 0::numeric
        END                                                                                AS mean_plan_ms,
        s.query,
        COALESCE(wq.wait_samples, 0)                                                       AS wait_samples,
        COALESCE(wq.io_wait_fraction, 0)                                                   AS io_wait_fraction,
        COALESCE(wq.lock_wait_fraction, 0)                                                 AS lock_wait_fraction,
        COALESCE(wq.ipc_wait_fraction, 0)                                                  AS ipc_wait_fraction,
        COALESCE(cn.is_cross_node, 0)                                                      AS is_cross_node
    FROM pg_stat_statements AS s
    LEFT JOIN _pss_wait_by_query AS wq
           ON wq.queryid = s.queryid
    LEFT JOIN _pss_query_crossnode AS cn
           ON cn.queryid = s.queryid
    WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      -- Only classify top-level statements.
      AND s.toplevel
      -- Exclude this classifier's own statements so it does not profile itself.
      AND s.query !~* '_pss_wait_by_query|_pss_workload_features|pgms_wait_sampling_view|_pss_shard_fairness|_pss_query_crossnode|_pss_query_locality|_pss_final_score|citus_stat_statements'
      -- Exclude transaction-control, session, and DDL/utility commands that are
      -- not part of the application workload (keeps the analysis to real DML).
      AND s.query !~* '^\s*(begin|start|commit|end|rollback|savepoint|release|prepare|deallocate|set|reset|show|discard|lock|listen|unlisten|notify|create|drop|alter|truncate|vacuum|analyze|analyse|reindex|cluster|refresh|grant|revoke|comment|security|do|call|explain|copy)\M'
),
features AS (
    SELECT
        *,
        total_plan_ms / NULLIF(total_exec_ms, 0)                                            AS plan_exec_ratio,
        total_plan_ms / NULLIF(total_plan_ms + total_exec_ms, 0)                             AS plan_fraction,
        temp_blks_total / NULLIF(shared_blks_total + temp_blks_total + local_blks_total, 0) AS temp_io_fraction,
        shared_blks_total / NULLIF(calls, 0)                                                 AS shared_blks_per_call,
        temp_blks_total / NULLIF(calls, 0)                                                   AS temp_blks_per_call,
        CASE WHEN query ~* '^\s*select\m' THEN 1 ELSE 0 END                                AS is_select,
        CASE WHEN query ~* '^\s*insert\m' THEN 1 ELSE 0 END                                AS is_insert,
        CASE WHEN query ~* '^\s*update\m' THEN 1 ELSE 0 END                                AS is_update,
        CASE WHEN query ~* '^\s*delete\m' THEN 1 ELSE 0 END                                AS is_delete,
        CASE WHEN query ~* '\mjoin\M' THEN 1 ELSE 0 END                                    AS has_join,
        CASE WHEN query ~* '\mgroup\s+by\M' THEN 1 ELSE 0 END                             AS has_group_by,
        CASE WHEN query ~* '\mdistinct\M' THEN 1 ELSE 0 END                                AS has_distinct,
        CASE WHEN query ~* '\morder\s+by\M' THEN 1 ELSE 0 END                             AS has_order_by,
        CASE WHEN query ~* '\mhaving\M' THEN 1 ELSE 0 END                                  AS has_having,
        CASE WHEN query ~* '\mover\s*\(' THEN 1 ELSE 0 END                                AS has_window,
        CASE WHEN query ~* '\mlimit\M' THEN 1 ELSE 0 END                                   AS has_limit,
        CASE WHEN query ~* '\m(date_trunc|time_bucket|extract\s*\(|now\s*\(|current_timestamp|current_date)\M' THEN 1 ELSE 0 END
                                                                                             AS has_time_function,
        CASE WHEN query ~* '\m(created_at|updated_at|event_time|event_ts|timestamp|ts|time)\M' THEN 1 ELSE 0 END
                                                                                             AS has_time_column_hint,
        CASE WHEN query ~* '\minterval\M' THEN 1 ELSE 0 END                                AS has_interval,
        CASE WHEN query ~* '\morder\s+by\s+[^;]*\m(desc)\M' AND query ~* '\mlimit\M' THEN 1 ELSE 0 END
                                                                                             AS recent_window_pattern
    FROM base
),
scored AS (
    SELECT
        *,

        (
            2.0 * CASE WHEN mean_exec_ms < 20 THEN 1 ELSE 0 END
          + 1.5 * CASE WHEN rows_per_call < 100 THEN 1 ELSE 0 END
          + 1.2 * CASE WHEN (is_insert + is_update + is_delete) > 0 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN has_group_by + has_distinct + has_window + has_having = 0 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN temp_io_fraction < 0.02 OR temp_io_fraction IS NULL THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN calls >= 100 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN plan_fraction > 0.15 THEN 1 ELSE 0 END
          + 0.4 * CASE WHEN has_limit = 1 THEN 1 ELSE 0 END
          + 0.6 * CASE WHEN lock_wait_fraction > 0.30 THEN 1 ELSE 0 END
        )::numeric(20,6)                                                                     AS oltp_raw,

        (
            2.0 * CASE WHEN mean_exec_ms > 200 THEN 1 ELSE 0 END
          + 1.5 * CASE WHEN rows_per_call > 1000 THEN 1 ELSE 0 END
          + 1.2 * CASE WHEN has_group_by + has_distinct + has_window + has_having > 0 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN has_join = 1 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN temp_io_fraction > 0.05 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN shared_blks_per_call > 1000 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN plan_fraction < 0.05 THEN 1 ELSE 0 END
          + 0.4 * CASE WHEN is_select = 1 THEN 1 ELSE 0 END
          + 0.6 * CASE WHEN io_wait_fraction > 0.30 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN is_cross_node = 1 THEN 1 ELSE 0 END
        )::numeric(20,6)                                                                     AS olap_raw,

        (
            2.0 * CASE WHEN is_insert = 1 THEN 1 ELSE 0 END
          + 1.4 * CASE WHEN has_time_function = 1 OR has_time_column_hint = 1 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN has_interval = 1 THEN 1 ELSE 0 END
          + 1.0 * CASE WHEN recent_window_pattern = 1 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN rows_per_call BETWEEN 1 AND 100000 THEN 1 ELSE 0 END
          + 0.6 * CASE WHEN has_group_by = 1 AND (has_time_function = 1 OR has_time_column_hint = 1) THEN 1 ELSE 0 END
        )::numeric(20,6)                                                                     AS timeseries_raw,

        (
            2.0 * LEAST(
                    (
                        (2.0 * CASE WHEN mean_exec_ms < 20 THEN 1 ELSE 0 END)
                      + (1.5 * CASE WHEN rows_per_call < 100 THEN 1 ELSE 0 END)
                      + (1.2 * CASE WHEN (is_insert + is_update + is_delete) > 0 THEN 1 ELSE 0 END)
                    ) / 4.7,
                    (
                        (2.0 * CASE WHEN mean_exec_ms > 200 THEN 1 ELSE 0 END)
                      + (1.5 * CASE WHEN rows_per_call > 1000 THEN 1 ELSE 0 END)
                      + (1.2 * CASE WHEN has_group_by + has_distinct + has_window + has_having > 0 THEN 1 ELSE 0 END)
                    ) / 4.7
                )
          + 0.8 * CASE WHEN plan_fraction BETWEEN 0.05 AND 0.30 THEN 1 ELSE 0 END
          + 0.8 * CASE WHEN calls >= 10 AND mean_exec_ms >= 5 THEN 1 ELSE 0 END
          + 0.6 * CASE WHEN has_join = 1 OR has_group_by = 1 OR (is_insert + is_update + is_delete) > 0 THEN 1 ELSE 0 END
          + 0.5 * CASE WHEN ipc_wait_fraction > 0.20 THEN 1 ELSE 0 END
        )::numeric(20,6)                                                                     AS htap_raw
    FROM features
),
weighted AS (
    SELECT
        *,
        calls                                                                                 AS call_weight,
        total_exec_ms                                                                         AS exec_weight,
        (shared_blks_total + temp_blks_total + local_blks_total)                             AS io_weight
    FROM scored
)
SELECT *
FROM weighted;

\if :verbose
-- Result set 1: fingerprint-level scores
SELECT
    queryid,
    calls,
    round(total_exec_ms, 2)                       AS total_exec_ms,
    round(mean_exec_ms, 4)                        AS mean_exec_ms,
    round(total_plan_ms, 2)                       AS total_plan_ms,
    round(mean_plan_ms, 4)                        AS mean_plan_ms,
    round(plan_fraction, 4)                       AS plan_fraction,
    round(plan_exec_ratio, 4)                     AS plan_exec_ratio,
    round(rows_per_call, 2)                       AS rows_per_call,
    round(shared_blks_per_call, 2)                AS shared_blks_per_call,
    round(temp_blks_per_call, 2)                  AS temp_blks_per_call,
    round(temp_io_fraction, 4)                    AS temp_io_fraction,
    round(wait_samples, 0)                        AS wait_samples,
    round(io_wait_fraction, 4)                    AS io_wait_fraction,
    round(lock_wait_fraction, 4)                  AS lock_wait_fraction,
    round(ipc_wait_fraction, 4)                   AS ipc_wait_fraction,
    round(oltp_raw, 2)                            AS oltp_raw,
    round(olap_raw, 2)                            AS olap_raw,
    round(htap_raw, 2)                            AS htap_raw,
    round(timeseries_raw, 2)                      AS timeseries_raw,
    left(regexp_replace(query, E'\\s+', ' ', 'g'), 220) AS query_sample
FROM _pss_workload_features
ORDER BY total_exec_ms DESC, calls DESC;

-- Result set 2: aggregate percentages by weighting model
WITH agg AS (
    SELECT
        'calls'::text AS weighting,
        sum(oltp_raw       * call_weight) AS oltp_w,
        sum(olap_raw       * call_weight) AS olap_w,
        sum(htap_raw       * call_weight) AS htap_w,
        sum(timeseries_raw * call_weight) AS timeseries_w
    FROM _pss_workload_features

    UNION ALL

    SELECT
        'execution_time_ms'::text AS weighting,
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM _pss_workload_features

    UNION ALL

    SELECT
        'io_blocks'::text AS weighting,
        sum(oltp_raw       * io_weight) AS oltp_w,
        sum(olap_raw       * io_weight) AS olap_w,
        sum(htap_raw       * io_weight) AS htap_w,
        sum(timeseries_raw * io_weight) AS timeseries_w
    FROM _pss_workload_features
),
norm AS (
    SELECT
        *,
        (oltp_w + olap_w + htap_w + timeseries_w) AS total_w
    FROM agg
)
SELECT
    weighting,
    round(100 * oltp_w       / NULLIF(total_w, 0), 2) AS oltp_pct,
    round(100 * olap_w       / NULLIF(total_w, 0), 2) AS olap_pct,
    round(100 * htap_w       / NULLIF(total_w, 0), 2) AS htap_pct,
    round(100 * timeseries_w / NULLIF(total_w, 0), 2) AS timeseries_pct
FROM norm
ORDER BY CASE weighting
           WHEN 'calls' THEN 1
           WHEN 'execution_time_ms' THEN 2
           ELSE 3
         END;

-- Result set 3: planning-bound vs execution-bound summary
SELECT
    round(sum(calls) FILTER (WHERE plan_fraction >= 0.30) / NULLIF(sum(calls), 0) * 100, 2) AS planning_bound_calls_pct,
    round(sum(total_exec_ms) FILTER (WHERE plan_fraction >= 0.30) / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS planning_bound_exec_pct,
    round(sum(calls) FILTER (WHERE plan_fraction <= 0.05) / NULLIF(sum(calls), 0) * 100, 2) AS execution_bound_calls_pct,
    round(sum(total_exec_ms) FILTER (WHERE plan_fraction <= 0.05) / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS execution_bound_exec_pct,
    round(sum(calls) FILTER (WHERE mean_exec_ms < 20 AND plan_fraction >= 0.30) / NULLIF(sum(calls), 0) * 100, 2) AS oltp_like_planning_heavy_calls_pct,
    round(sum(total_exec_ms) FILTER (
        WHERE mean_exec_ms > 200
          AND (has_group_by = 1 OR has_window = 1 OR has_distinct = 1 OR has_having = 1)
    ) / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS olap_like_exec_heavy_exec_pct
FROM _pss_workload_features;

-- Result set 4: pgms_wait_sampling wait-event category summary.
-- All zeros / no rows when the wait-sampling view was not reachable.
SELECT
    coalesce(sum(wait_samples), 0)                                                          AS total_wait_samples,
    count(*) FILTER (WHERE wait_samples > 0)                                                AS queries_with_waits,
    round(sum(io_wait_fraction   * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS io_wait_pct,
    round(sum(lock_wait_fraction * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS lock_wait_pct,
    round(sum(ipc_wait_fraction  * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS ipc_wait_pct
FROM _pss_workload_features;\endif
-- Optional Citus data-distribution fairness.
-- Measures how evenly each distributed table's shard bytes are spread across
-- worker nodes using a coefficient-of-variation (CV) fairness score:
--     fairness = 1 - stddev_pop(x_i) / mean(x_i)   (clamped to [0, 1])
-- where x_i is the total shard bytes on node i. The score is 1.00 when every
-- node holds the same bytes (population stddev 0) and falls toward 0 as the
-- spread grows, so it punishes imbalance harder than Jain's index. Example: a
-- table with 100 bytes on n1 and 200 on n2 has mean 150 and population stddev
-- 50, scoring  1 - 50 / 150 = 0.67.
-- The table is always created so the result sets work; it stays empty when this
-- is not a Citus cluster (citus_shards absent), and a NOTICE is raised.
SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_shard_fairness;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_shard_fairness (
    table_name                  text,
    node_count                  integer,
    total_bytes                 numeric,
    data_distribution_fairness  numeric(4,2)
);

DO $plpgsql$
DECLARE
    v_relid oid := to_regclass('pg_catalog.citus_shards');
BEGIN
    IF v_relid IS NULL THEN
        RAISE NOTICE 'Citus not detected (citus_shards absent); data distribution fairness disabled.';
        RETURN;
    END IF;

    -- Per-distributed-table fairness across nodes.
    INSERT INTO _pss_shard_fairness
    WITH per_node AS (
        SELECT
            table_name::text          AS table_name,
            nodename,
            nodeport,
            sum(shard_size)::numeric  AS node_bytes
        FROM citus_shards
        GROUP BY table_name, nodename, nodeport
    )
    SELECT
        table_name,
        count(*)                                                                           AS node_count,
        sum(node_bytes)                                                                    AS total_bytes,
        CASE
            WHEN sum(node_bytes) = 0 THEN 1.00
            ELSE round(GREATEST(0, 1 - stddev_pop(node_bytes) / NULLIF(avg(node_bytes), 0)), 2)
        END                                                                                AS data_distribution_fairness
    FROM per_node
    GROUP BY table_name;

    -- Cluster-wide fairness: total shard bytes on each node, across all tables.
    INSERT INTO _pss_shard_fairness
    WITH per_node_all AS (
        SELECT
            nodename,
            nodeport,
            sum(shard_size)::numeric AS node_bytes
        FROM citus_shards
        GROUP BY nodename, nodeport
    )
    SELECT
        '(cluster overall)',
        count(*),
        sum(node_bytes),
        CASE
            WHEN sum(node_bytes) = 0 THEN 1.00
            ELSE round(GREATEST(0, 1 - stddev_pop(node_bytes) / NULLIF(avg(node_bytes), 0)), 2)
        END
    FROM per_node_all;

    RAISE NOTICE 'Citus data distribution fairness computed for % distributed table(s).',
        (SELECT count(*) FROM _pss_shard_fairness WHERE table_name <> '(cluster overall)');
END
$plpgsql$;

\if :verbose
-- Result set 5: per-table data distribution fairness (Citus).
-- Empty when not running on a Citus cluster.
SELECT
    table_name,
    node_count,
    round(total_bytes, 0)        AS total_bytes,
    data_distribution_fairness
FROM _pss_shard_fairness
ORDER BY (table_name = '(cluster overall)'), data_distribution_fairness ASC, total_bytes DESC;

-- Result set 6: final workload scores (execution-time weighted) plus the
-- cluster-wide data distribution fairness in a single summary row.
WITH scores AS (
    SELECT
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM _pss_workload_features
),
norm AS (
    SELECT
        *,
        (oltp_w + olap_w + htap_w + timeseries_w) AS total_w
    FROM scores
)
SELECT
    round(100 * oltp_w       / NULLIF(total_w, 0), 2) AS oltp_pct,
    round(100 * olap_w       / NULLIF(total_w, 0), 2) AS olap_pct,
    round(100 * htap_w       / NULLIF(total_w, 0), 2) AS htap_pct,
    round(100 * timeseries_w / NULLIF(total_w, 0), 2) AS timeseries_pct,
    (SELECT data_distribution_fairness
       FROM _pss_shard_fairness
      WHERE table_name = '(cluster overall)')        AS data_distribution_fairness
FROM norm;
\endif

-- Query routability / locality.
-- For every application statement (top-level AND nested) build a routability
-- score in (0, 1]. It starts at 1.00 (ideal: no parent, single node) and is
-- multiplied by a scale factor for each problem - 0.5 if the query has a parent
-- (not top-level) and 0.5 if it runs cross-node - so the score is penalized but
-- never reaches 0 (worst case, both issues = 0.25). has_parent comes from
-- pg_stat_statements.toplevel and is_cross_node from the Citus rollup above.
-- Unlike the main classifier this set does NOT filter out nested statements, so
-- the parent signal is visible; it still excludes the classifier's own
-- scaffolding and administrative/DDL noise.
SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_query_locality;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_query_locality AS
SELECT
    s.queryid,
    s.calls::numeric                                               AS calls,
    s.total_exec_time::numeric                                     AS total_exec_ms,
    CASE WHEN s.toplevel THEN 0 ELSE 1 END                         AS has_parent,
    COALESCE(cn.is_cross_node, 0)                                  AS is_cross_node,
    round(
        1.00
        * CASE WHEN s.toplevel THEN 1.0 ELSE 0.5 END                       -- parent penalty
        * CASE WHEN COALESCE(cn.is_cross_node, 0) = 1 THEN 0.5 ELSE 1.0 END -- cross-node penalty
    , 2)::numeric(4,2)                                             AS routability_score,
    s.query
FROM pg_stat_statements AS s
LEFT JOIN _pss_query_crossnode AS cn
       ON cn.queryid = s.queryid
WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND s.query !~* '_pss_wait_by_query|_pss_workload_features|pgms_wait_sampling_view|_pss_shard_fairness|_pss_query_crossnode|_pss_query_locality|_pss_final_score|citus_stat_statements'
  AND s.query !~* '^\s*(begin|start|commit|end|rollback|savepoint|release|prepare|deallocate|set|reset|show|discard|lock|listen|unlisten|notify|create|drop|alter|truncate|vacuum|analyze|analyse|reindex|cluster|refresh|grant|revoke|comment|security|do|call|explain|copy)\M';

\if :verbose
-- Result set 7: per-query routability (worst first).
SELECT
    queryid,
    calls,
    round(total_exec_ms, 2)                       AS total_exec_ms,
    has_parent,
    is_cross_node,
    routability_score,
    left(regexp_replace(query, E'\\s+', ' ', 'g'), 220) AS query_sample
FROM _pss_query_locality
ORDER BY routability_score ASC, total_exec_ms DESC;

-- Result set 8: routability summary by share of execution time.
SELECT
    round(sum(total_exec_ms) FILTER (WHERE has_parent = 0 AND is_cross_node = 0)
          / NULLIF(sum(total_exec_ms), 0) * 100, 2)                                         AS ideal_local_exec_pct,
    round(sum(total_exec_ms) FILTER (WHERE is_cross_node = 1)
          / NULLIF(sum(total_exec_ms), 0) * 100, 2)                                         AS cross_node_exec_pct,
    round(sum(total_exec_ms) FILTER (WHERE has_parent = 1)
          / NULLIF(sum(total_exec_ms), 0) * 100, 2)                                         AS has_parent_exec_pct,
    round(avg(routability_score), 2)                                                        AS mean_routability_score
FROM _pss_query_locality;
\endif

-- Result set 9: the whole score in a single final row - execution-time weighted
-- workload mix, Citus data distribution fairness, and routability headline
-- metrics combined for an at-a-glance summary. It is materialized into
-- _pss_final_score (with a collected_at timestamp) so the same row can be both
-- displayed and appended to the Grafana CSV below.
SET LOCAL client_min_messages = warning;
DROP TABLE IF EXISTS _pss_final_score;
SET LOCAL client_min_messages = notice;
CREATE TEMP TABLE _pss_final_score AS
WITH scores AS (
    SELECT
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM _pss_workload_features
),
norm AS (
    SELECT
        *,
        (oltp_w + olap_w + htap_w + timeseries_w) AS total_w
    FROM scores
),
locality AS (
    SELECT
        round(sum(total_exec_ms) FILTER (WHERE has_parent = 0 AND is_cross_node = 0)
              / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS ideal_local_exec_pct,
        round(sum(total_exec_ms) FILTER (WHERE is_cross_node = 1)
              / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS cross_node_exec_pct,
        round(sum(total_exec_ms) FILTER (WHERE has_parent = 1)
              / NULLIF(sum(total_exec_ms), 0) * 100, 2) AS has_parent_exec_pct,
        round(avg(routability_score), 2)                AS mean_routability_score
    FROM _pss_query_locality
),
base AS (
    SELECT
        now()                                                      AS collected_at,
        round(100 * norm.oltp_w       / NULLIF(norm.total_w, 0), 2) AS oltp_pct,
        round(100 * norm.olap_w       / NULLIF(norm.total_w, 0), 2) AS olap_pct,
        round(100 * norm.htap_w       / NULLIF(norm.total_w, 0), 2) AS htap_pct,
        round(100 * norm.timeseries_w / NULLIF(norm.total_w, 0), 2) AS timeseries_pct,
        (SELECT data_distribution_fairness
           FROM _pss_shard_fairness
          WHERE table_name = '(cluster overall)')                  AS data_distribution_fairness,
        locality.ideal_local_exec_pct,
        locality.cross_node_exec_pct,
        locality.mean_routability_score
    FROM norm CROSS JOIN locality
)
-- Combine workload type (justifies scaling) with routability and shard
-- fairness (confirm the cluster can absorb it) into a single, tunable verdict.
SELECT
    base.collected_at,
    base.oltp_pct,
    base.olap_pct,
    base.htap_pct,
    base.timeseries_pct,
    base.data_distribution_fairness,
    base.ideal_local_exec_pct,
    base.cross_node_exec_pct,
    base.mean_routability_score,
    -- Dominant workload type by execution-time-weighted share.
    CASE GREATEST(COALESCE(base.oltp_pct, 0), COALESCE(base.olap_pct, 0),
                  COALESCE(base.htap_pct, 0), COALESCE(base.timeseries_pct, 0))
        WHEN COALESCE(base.oltp_pct, 0)       THEN 'OLTP'
        WHEN COALESCE(base.olap_pct, 0)       THEN 'OLAP'
        WHEN COALESCE(base.htap_pct, 0)       THEN 'HTAP'
        ELSE 'TIME_SERIES'
    END                                                            AS dominant_workload,
    -- Scale-out verdict. Gates (skew, then locality) are checked before the
    -- positive scale-out signals, because a skewed or scatter-gather workload
    -- must be fixed before adding nodes helps. Thresholds are tunable.
    CASE
        WHEN COALESCE(base.oltp_pct, 0) + COALESCE(base.olap_pct, 0)
           + COALESCE(base.htap_pct, 0) + COALESCE(base.timeseries_pct, 0) = 0
            THEN 'NO_DATA: no classifiable workload captured (idle DB or stats reset).'
        WHEN base.data_distribution_fairness IS NOT NULL AND base.data_distribution_fairness < 0.85
            THEN 'REBALANCE_FIRST: shard bytes are skewed across nodes (fairness '
                 || base.data_distribution_fairness || '); rebalance / fix the distribution key before adding nodes.'
        WHEN COALESCE(base.cross_node_exec_pct, 0) >= 40 OR COALESCE(base.mean_routability_score, 1) < 0.60
            THEN 'OPTIMIZE_LOCALITY_FIRST: scatter-gather heavy (cross-node '
                 || COALESCE(base.cross_node_exec_pct, 0) || '%); improve co-location / reference tables before scaling out.'
        WHEN COALESCE(base.olap_pct, 0) + COALESCE(base.htap_pct, 0) >= 40
            THEN 'SCALE_OUT: analytical/mixed demand with good routability and balanced shards; adding worker nodes should scale near-linearly.'
        WHEN COALESCE(base.oltp_pct, 0) >= 50 AND COALESCE(base.ideal_local_exec_pct, 0) >= 60
            THEN 'SCALE_OUT: routable OLTP throughput; single-shard queries spread cleanly across nodes (Citus MX).'
        WHEN COALESCE(base.oltp_pct, 0) >= 50
            THEN 'SCALE_UP_OR_TUNE: predominantly OLTP; prefer vertical scale / pooling / indexing unless throughput-bound, then shard by tenant key.'
        ELSE 'REVIEW: mixed signals; inspect the detailed result sets (run with -v verbose=on).'
    END                                                            AS scale_recommendation
FROM base;

SELECT
    oltp_pct,
    olap_pct,
    htap_pct,
    timeseries_pct,
    data_distribution_fairness,
    ideal_local_exec_pct,
    cross_node_exec_pct,
    mean_routability_score,
    dominant_workload,
    scale_recommendation
FROM _pss_final_score;

-- CSV export for Grafana: append the final score as one timestamped row,
-- writing the column header only when the file is new/empty. The path comes
-- from the psql ':csv' variable, passed to the server through a custom GUC so a
-- plpgsql COPY ... TO PROGRAM can use it. COPY ... TO PROGRAM needs superuser or
-- the pg_execute_server_program role; on failure the export is skipped with a
-- NOTICE and the rest of the script still succeeds.
-- NOTE: COPY ... TO PROGRAM runs on the SERVER, so a relative path resolves
-- against the server's data_directory, not the directory psql was launched from.
-- Pass an absolute path (-v csv=/abs/path.csv) for a predictable location.
SET pss.csv = :'csv';
DO $plpgsql$
DECLARE
    v_path     text := current_setting('pss.csv', true);
    v_abs_path text;
    v_hdr  text := 'collected_at,oltp_pct,olap_pct,htap_pct,timeseries_pct,data_distribution_fairness,ideal_local_exec_pct,cross_node_exec_pct,mean_routability_score,dominant_workload,scale_recommendation';
    v_prog text;
BEGIN
    IF v_path IS NULL OR v_path = '' THEN
        RAISE NOTICE 'CSV export skipped: no output path configured (set -v csv=/path/file.csv).';
        RETURN;
    END IF;

    -- Resolve the path for reporting: relative paths land in the data directory.
    v_abs_path := CASE
                      WHEN left(v_path, 1) = '/' THEN v_path
                      ELSE current_setting('data_directory') || '/' || v_path
                  END;

    -- Shell: create the header once (when the file is missing or empty), then
    -- append the CSV rows COPY streams in on stdin.
    v_prog := format($p$f="%s"; test -s "$f" || printf '%%s\n' '%s' > "$f"; cat >> "$f"$p$, v_path, v_hdr);

    BEGIN
        EXECUTE format(
            'COPY (SELECT collected_at, oltp_pct, olap_pct, htap_pct, timeseries_pct, '
            'data_distribution_fairness, ideal_local_exec_pct, cross_node_exec_pct, '
            'mean_routability_score, dominant_workload, scale_recommendation '
            'FROM _pss_final_score) TO PROGRAM %L WITH (FORMAT csv)',
            v_prog
        );
        RAISE NOTICE 'Workload score appended to CSV for Grafana: %', v_abs_path;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'CSV export failed (%); COPY ... TO PROGRAM needs superuser or pg_execute_server_program. Path: %',
            SQLERRM, v_abs_path;
    END;
END
$plpgsql$;

COMMIT;
