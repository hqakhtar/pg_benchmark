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
--   * TIME_SERIES is treated as an orthogonal pattern; percentages are normalized
--     across OLTP / OLAP / HTAP / TIME_SERIES for convenience.
--   * Scoring thresholds are intentionally simple and tunable.
--
-- Run with:
--   psql -f workload_score_pg_stat_statements.sql

\pset tuples_only off
\pset pager off

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
DROP TABLE IF EXISTS _pss_wait_by_query;
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

DROP TABLE IF EXISTS _pss_workload_features;
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
        COALESCE(wq.ipc_wait_fraction, 0)                                                  AS ipc_wait_fraction
    FROM pg_stat_statements AS s
    LEFT JOIN _pss_wait_by_query AS wq
           ON wq.queryid = s.queryid
    WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
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
FROM _pss_workload_features;

COMMIT;
