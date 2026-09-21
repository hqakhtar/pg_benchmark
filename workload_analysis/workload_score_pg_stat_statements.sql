-- Legacy psql compatibility runner for FSPG EC Advisor.
-- Canonical SQL-only extension: workload_analysis/fspg_ec_advisor
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
--   * PostgreSQL 17. Planning signals contribute only while
--     pg_stat_statements.track_planning is enabled.
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

-- Phase 1 advisor thresholds. Override through psql variables when a workload
-- needs a different capture cadence or pressure definition.
\if :{?advisor_pressure_samples}
\else
\set advisor_pressure_samples 3
\endif

\if :{?advisor_pressure_window_minutes}
\else
\set advisor_pressure_window_minutes 60
\endif

\if :{?advisor_min_window_seconds}
\else
\set advisor_min_window_seconds 300
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

-- Phase 1 keeps durable advisor state in a dedicated schema. These small
-- coordinator-local tables retain capture baselines and operator outcomes;
-- per-run calculations remain in pg_temp tables below.
SET LOCAL client_min_messages = warning;
CREATE SCHEMA IF NOT EXISTS citus_advisor;

CREATE TABLE IF NOT EXISTS citus_advisor.capture_history (
    capture_key                    text PRIMARY KEY,
    collected_at                   timestamptz NOT NULL,
    server_id                      text NOT NULL,
    database_oid                   integer NOT NULL,
    database_name                  text NOT NULL,
    pgss_stats_reset               timestamptz,
    database_stats_reset           timestamptz,
    client_io_stats_reset          timestamptz,
    checkpointer_stats_reset       timestamptz,
    workload_calls_total           numeric NOT NULL,
    workload_exec_ms_total         numeric NOT NULL,
    workload_io_blocks_total       numeric NOT NULL,
    resource_pressure_status       text NOT NULL,
    connection_utilization_pct     numeric,
    waiting_active_pct             numeric,
    database_temp_bytes            numeric,
    database_deadlocks             numeric,
    client_io_reads                numeric,
    client_io_writes               numeric,
    client_io_read_time_ms         numeric,
    client_io_write_time_ms        numeric,
    checkpoints_timed              numeric,
    checkpoints_requested          numeric,
    checkpoint_write_time_ms       numeric,
    checkpoint_sync_time_ms        numeric,
    recommended_action             text NOT NULL,
    recommendation_confidence      text NOT NULL,
    telemetry_coverage_pct         numeric NOT NULL,
    blocking_signals               text,
    evidence_summary               text,
    expected_benefit               text,
    verification_step              text
);

CREATE INDEX IF NOT EXISTS capture_history_server_database_collected_at_idx
    ON citus_advisor.capture_history (server_id, database_oid, collected_at DESC);

CREATE TABLE IF NOT EXISTS citus_advisor.recommendation_feedback (
    feedback_id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    capture_key                    text NOT NULL
        REFERENCES citus_advisor.capture_history (capture_key)
        ON DELETE CASCADE,
    recorded_at                    timestamptz NOT NULL DEFAULT now(),
    operator_action                text NOT NULL,
    outcome                        text NOT NULL,
    notes                          text
);

CREATE OR REPLACE FUNCTION citus_advisor.record_feedback(
    p_capture_key text,
    p_operator_action text,
    p_outcome text,
    p_notes text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
AS $plpgsql$
DECLARE
    v_feedback_id bigint;
BEGIN
    INSERT INTO citus_advisor.recommendation_feedback (
        capture_key,
        operator_action,
        outcome,
        notes
    )
    VALUES (
        p_capture_key,
        p_operator_action,
        p_outcome,
        p_notes
    )
    RETURNING feedback_id INTO v_feedback_id;

    RETURN v_feedback_id;
END
$plpgsql$;

CREATE OR REPLACE VIEW citus_advisor.recommendation_calibration AS
SELECT
    history.recommended_action,
    history.recommendation_confidence,
    count(feedback.feedback_id) AS feedback_count,
    count(feedback.feedback_id) FILTER (
        WHERE feedback.outcome IN ('success', 'improved', 'confirmed_benefit')
    ) AS positive_outcome_count,
    round(
        100.0
        * count(feedback.feedback_id) FILTER (
            WHERE feedback.outcome IN ('success', 'improved', 'confirmed_benefit')
        )
        / NULLIF(count(feedback.feedback_id), 0),
        2
    ) AS positive_outcome_pct
FROM citus_advisor.capture_history AS history
LEFT JOIN citus_advisor.recommendation_feedback AS feedback
       ON feedback.capture_key = history.capture_key
GROUP BY
    history.recommended_action,
    history.recommendation_confidence;
SET LOCAL client_min_messages = notice;

-- PostgreSQL requires temporary relations to live in its per-session pg_temp
-- schema. Give every scratch relation a Citus-workload namespace plus a run ID
-- derived from the PostgreSQL system identifier, database, and timestamp.
-- The fallback keeps the script usable for roles that cannot read control-file
-- metadata, which is common on managed PostgreSQL services.
DO $plpgsql$
DECLARE
    v_server_id text;
    v_database_id text;
    v_database_oid integer;
    v_run_tag text;
BEGIN
    BEGIN
        SELECT system_identifier::text
        INTO v_server_id
        FROM pg_control_system();
    EXCEPTION WHEN insufficient_privilege THEN
        v_server_id := coalesce(inet_server_addr()::text, 'local')
                       || '_' || coalesce(inet_server_port()::text, '0');
    END;

    v_server_id := left(regexp_replace(lower(v_server_id), '[^a-z0-9]+', '_', 'g'), 20);
    v_database_id := left(regexp_replace(lower(current_database()), '[^a-z0-9]+', '_', 'g'), 10);
    SELECT oid::integer
    INTO v_database_oid
    FROM pg_database
    WHERE datname = current_database();
    v_run_tag := format(
        'citus_%s_%s_%s',
        coalesce(nullif(v_server_id, ''), 'server'),
        coalesce(nullif(v_database_id, ''), 'database'),
        to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS')
    );

    PERFORM set_config('pss.run_tag', v_run_tag, true);
    PERFORM set_config('citus_advisor.server_id', v_server_id, true);
    PERFORM set_config('citus_advisor.database_oid', v_database_oid::text, true);
    PERFORM set_config('citus_advisor.database_name', current_database(), true);
END
$plpgsql$;

SELECT current_setting('pss.run_tag') AS pss_run_tag
\gset

SELECT
    format('%s_wait', :'pss_run_tag') AS pss_wait_table,
    format('%s_xnode', :'pss_run_tag') AS pss_crossnode_table,
    format('%s_feat', :'pss_run_tag') AS pss_features_table,
    format('%s_fair', :'pss_run_tag') AS pss_fairness_table,
    format('%s_local', :'pss_run_tag') AS pss_locality_table,
    format('%s_locsum', :'pss_run_tag') AS pss_locality_summary_table,
    format('%s_res', :'pss_run_tag') AS pss_resource_table,
    format('%s_adv', :'pss_run_tag') AS pss_advisor_table,
    format('%s_final', :'pss_run_tag') AS pss_final_table
    , format('%s_out', :'pss_run_tag') AS pss_output_table
\gset

SELECT format(
    '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|citus_advisor|pgms_wait_sampling_view|citus_stat_statements',
    :'pss_wait_table',
    :'pss_crossnode_table',
    :'pss_features_table',
    :'pss_fairness_table',
    :'pss_locality_table',
    :'pss_locality_summary_table',
    :'pss_resource_table',
    :'pss_advisor_table',
    :'pss_final_table',
    :'pss_output_table'
) AS pss_internal_object_pattern
\gset

SET LOCAL pss.final_table = :'pss_final_table';
SET LOCAL citus_advisor.pressure_samples = :'advisor_pressure_samples';
SET LOCAL citus_advisor.pressure_window_minutes = :'advisor_pressure_window_minutes';
SET LOCAL citus_advisor.min_window_seconds = :'advisor_min_window_seconds';

-- Optional pgms_wait_sampling integration.
-- Build a per-query wait-event rollup keyed by queryid. The table is always
-- created so downstream LEFT JOINs work; it stays empty (all-zero signals) when
-- the Query Store wait-sampling view is not reachable in this database.
CREATE TEMP TABLE :"pss_wait_table" (
    queryid             bigint,
    wait_samples        numeric,
    io_wait_samples     numeric,
    lock_wait_samples   numeric,
    ipc_wait_samples    numeric,
    client_wait_samples numeric,
    io_wait_fraction    numeric,
    lock_wait_fraction  numeric,
    ipc_wait_fraction   numeric
) ON COMMIT DROP;

SELECT to_regclass('query_store.pgms_wait_sampling_view') IS NOT NULL AS pss_has_wait_sampling
\gset

SET LOCAL citus_advisor.wait_sampling_available = :'pss_has_wait_sampling';

\if :pss_has_wait_sampling
    INSERT INTO :"pss_wait_table"
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
\else
\echo 'NOTICE: pgms_wait_sampling not reachable; wait signals disabled.'
\endif

-- Optional Citus cross-node detection (built before the classifier so the
-- cross-node signal can feed the OLAP score). citus_stat_statements exposes,
-- per fingerprint, the executor used and the routing key. A 'router' execution
-- with a partition key runs on a single node; anything else fans out across
-- nodes - a distributed scatter/gather that is treated as an OLAP signal. The
-- table is always created so downstream joins work; it stays empty when Citus
-- is absent (citus_stat_statements absent) and a NOTICE is raised, in which
-- case cross-node defaults to 0 (single node).
CREATE TEMP TABLE :"pss_crossnode_table" (
    queryid        bigint,
    is_cross_node  integer
) ON COMMIT DROP;

SELECT to_regclass('pg_catalog.citus_stat_statements') IS NOT NULL AS pss_has_citus_statements
\gset

SET LOCAL citus_advisor.citus_statements_available = :'pss_has_citus_statements';

\if :pss_has_citus_statements
    INSERT INTO :"pss_crossnode_table"
    SELECT
        queryid,
        max(CASE WHEN executor = 'router' AND partition_key IS NOT NULL THEN 0 ELSE 1 END) AS is_cross_node
    FROM pg_catalog.citus_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND queryid IS NOT NULL
    GROUP BY queryid;

\else
\echo 'NOTICE: Citus cross-node detection disabled; citus_stat_statements is absent.'
\endif

CREATE TEMP TABLE :"pss_features_table" ON COMMIT DROP AS
WITH settings AS (
    SELECT coalesce(current_setting('pg_stat_statements.track_planning', true)::boolean, false)
        AS planning_tracked
),
base AS (
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
        CASE WHEN settings.planning_tracked THEN s.total_plan_time::numeric END           AS total_plan_ms,
        CASE WHEN settings.planning_tracked THEN s.mean_plan_time::numeric END            AS mean_plan_ms,
        s.query,
        COALESCE(wq.wait_samples, 0)                                                       AS wait_samples,
        COALESCE(wq.io_wait_fraction, 0)                                                   AS io_wait_fraction,
        COALESCE(wq.lock_wait_fraction, 0)                                                 AS lock_wait_fraction,
        COALESCE(wq.ipc_wait_fraction, 0)                                                  AS ipc_wait_fraction,
        COALESCE(cn.is_cross_node, 0)                                                      AS is_cross_node
    FROM pg_stat_statements AS s
    CROSS JOIN settings
    LEFT JOIN :"pss_wait_table" AS wq
           ON wq.queryid = s.queryid
    LEFT JOIN :"pss_crossnode_table" AS cn
           ON cn.queryid = s.queryid
    WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      -- Only classify top-level statements.
      AND s.toplevel
            -- Exclude this classifier's own statements so it does not profile itself.
            AND s.query !~* :'pss_internal_object_pattern'
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
FROM :"pss_features_table"
ORDER BY total_exec_ms DESC, calls DESC;

-- Result set 2: aggregate percentages by weighting model
WITH agg AS (
    SELECT
        'calls'::text AS weighting,
        sum(oltp_raw       * call_weight) AS oltp_w,
        sum(olap_raw       * call_weight) AS olap_w,
        sum(htap_raw       * call_weight) AS htap_w,
        sum(timeseries_raw * call_weight) AS timeseries_w
    FROM :"pss_features_table"

    UNION ALL

    SELECT
        'execution_time_ms'::text AS weighting,
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM :"pss_features_table"

    UNION ALL

    SELECT
        'io_blocks'::text AS weighting,
        sum(oltp_raw       * io_weight) AS oltp_w,
        sum(olap_raw       * io_weight) AS olap_w,
        sum(htap_raw       * io_weight) AS htap_w,
        sum(timeseries_raw * io_weight) AS timeseries_w
    FROM :"pss_features_table"
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
FROM :"pss_features_table";

-- Result set 4: pgms_wait_sampling wait-event category summary.
-- All zeros / no rows when the wait-sampling view was not reachable.
SELECT
    coalesce(sum(wait_samples), 0)                                                          AS total_wait_samples,
    count(*) FILTER (WHERE wait_samples > 0)                                                AS queries_with_waits,
    round(sum(io_wait_fraction   * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS io_wait_pct,
    round(sum(lock_wait_fraction * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS lock_wait_pct,
    round(sum(ipc_wait_fraction  * wait_samples) / NULLIF(sum(wait_samples), 0) * 100, 2)   AS ipc_wait_pct
FROM :"pss_features_table";\endif
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
CREATE TEMP TABLE :"pss_fairness_table" (
    table_name                  text,
    node_count                  integer,
    total_bytes                 numeric,
    data_distribution_fairness  numeric(4,2)
) ON COMMIT DROP;

SELECT to_regclass('pg_catalog.citus_shards') IS NOT NULL
   AND to_regclass('pg_catalog.pg_dist_node') IS NOT NULL AS pss_has_citus_shards
\gset

\if :pss_has_citus_shards
    -- Per-distributed-table fairness across nodes.
    INSERT INTO :"pss_fairness_table"
    WITH workers AS (
        SELECT nodename, nodeport
        FROM pg_catalog.pg_dist_node
        WHERE groupid <> 0
          AND isactive
          AND noderole = 'primary'
    ),
    shard_bytes AS (
        SELECT
            table_name::text          AS table_name,
            nodename,
            nodeport,
            sum(shard_size)::numeric  AS node_bytes
        FROM pg_catalog.citus_shards
        GROUP BY table_name, nodename, nodeport
    ),
    tables AS (
        SELECT DISTINCT table_name
        FROM shard_bytes
    ),
    per_node AS (
        SELECT
            tables.table_name,
            workers.nodename,
            workers.nodeport,
            coalesce(shard_bytes.node_bytes, 0)::numeric AS node_bytes
        FROM tables
        CROSS JOIN workers
        LEFT JOIN shard_bytes
          ON shard_bytes.table_name = tables.table_name
         AND shard_bytes.nodename = workers.nodename
         AND shard_bytes.nodeport = workers.nodeport
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
    INSERT INTO :"pss_fairness_table"
    WITH workers AS (
        SELECT nodename, nodeport
        FROM pg_catalog.pg_dist_node
        WHERE groupid <> 0
          AND isactive
          AND noderole = 'primary'
    ),
    shard_bytes AS (
        SELECT
            nodename,
            nodeport,
            sum(shard_size)::numeric AS node_bytes
        FROM pg_catalog.citus_shards
        GROUP BY nodename, nodeport
    ),
    per_node_all AS (
        SELECT
            workers.nodename,
            workers.nodeport,
            coalesce(shard_bytes.node_bytes, 0)::numeric AS node_bytes
        FROM workers
        LEFT JOIN shard_bytes
          ON shard_bytes.nodename = workers.nodename
         AND shard_bytes.nodeport = workers.nodeport
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

\else
\echo 'NOTICE: Citus data-distribution fairness disabled; citus_shards is absent.'
\endif

\if :verbose
-- Result set 5: per-table data distribution fairness (Citus).
-- Empty when not running on a Citus cluster.
SELECT
    table_name,
    node_count,
    round(total_bytes, 0)        AS total_bytes,
    data_distribution_fairness
FROM :"pss_fairness_table"
ORDER BY (table_name = '(cluster overall)'), data_distribution_fairness ASC, total_bytes DESC;

-- Result set 6: final workload scores (execution-time weighted) plus the
-- cluster-wide data distribution fairness in a single summary row.
WITH scores AS (
    SELECT
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM :"pss_features_table"
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
    FROM :"pss_fairness_table"
      WHERE table_name = '(cluster overall)')        AS data_distribution_fairness
FROM norm;
\endif

-- Query routability / locality.
-- Use only top-level statements, matching the workload classifier and avoiding
-- duplicate execution time from nested statements that is already included in
-- their parent. Scores are aggregated by execution time, not fingerprint.
CREATE TEMP TABLE :"pss_locality_table" ON COMMIT DROP AS
SELECT
    s.queryid,
    s.calls::numeric                                               AS calls,
    s.total_exec_time::numeric                                     AS total_exec_ms,
    COALESCE(cn.is_cross_node, 0)                                  AS is_cross_node,
    round(
        1.00
        * CASE WHEN COALESCE(cn.is_cross_node, 0) = 1 THEN 0.5 ELSE 1.0 END
    , 2)::numeric(4,2)                                             AS routability_score,
    s.query
FROM pg_stat_statements AS s
LEFT JOIN :"pss_crossnode_table" AS cn
       ON cn.queryid = s.queryid
WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND s.toplevel
  AND s.query !~* :'pss_internal_object_pattern'
  AND s.query !~* '^\s*(begin|start|commit|end|rollback|savepoint|release|prepare|deallocate|set|reset|show|discard|lock|listen|unlisten|notify|create|drop|alter|truncate|vacuum|analyze|analyse|reindex|cluster|refresh|grant|revoke|comment|security|do|call|explain|copy)\M';

CREATE TEMP TABLE :"pss_locality_summary_table" ON COMMIT DROP AS
SELECT
    round(coalesce(sum(total_exec_ms) FILTER (WHERE is_cross_node = 0), 0)
        / NULLIF(sum(total_exec_ms), 0) * 100, 2)                                        AS ideal_local_exec_pct,
    round(coalesce(sum(total_exec_ms) FILTER (WHERE is_cross_node = 1), 0)
        / NULLIF(sum(total_exec_ms), 0) * 100, 2)                                        AS cross_node_exec_pct,
    round(sum(routability_score * total_exec_ms) / NULLIF(sum(total_exec_ms), 0), 2)       AS mean_routability_score
FROM :"pss_locality_table";

-- Resource-pressure snapshot. PostgreSQL statistics expose connection pressure
-- directly, but I/O and checkpoint counters are cumulative. Export the raw,
-- reset-aware counters so a dashboard can calculate their rates between runs.
CREATE TEMP TABLE :"pss_resource_table" ON COMMIT DROP AS
WITH connection_limits AS (
    SELECT
        coalesce((SELECT setting::integer FROM pg_settings WHERE name = 'max_connections'), 0)
            AS max_connections,
        coalesce((SELECT setting::integer FROM pg_settings WHERE name = 'reserved_connections'), 0)
            AS reserved_connections,
        coalesce((SELECT setting::integer FROM pg_settings WHERE name = 'superuser_reserved_connections'), 0)
            AS superuser_reserved_connections,
        coalesce((SELECT setting::boolean FROM pg_settings WHERE name = 'track_io_timing'), false)
            AS track_io_timing
),
session_snapshot AS (
    SELECT
        count(*) FILTER (WHERE backend_type = 'client backend')::integer AS client_backends,
        count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'active')::integer
            AS active_client_backends,
        count(*) FILTER (
            WHERE backend_type = 'client backend'
              AND state = 'active'
              AND wait_event_type IS NOT NULL
              AND wait_event_type NOT IN ('Client', 'Activity')
        )::integer AS waiting_client_backends,
        count(*) FILTER (
            WHERE backend_type = 'client backend'
              AND state = 'idle in transaction'
        )::integer AS idle_in_transaction_backends
    FROM pg_stat_activity
),
database_snapshot AS (
    SELECT
        blks_read::numeric AS database_blks_read,
        blks_hit::numeric AS database_blks_hit,
        temp_bytes::numeric AS database_temp_bytes,
        deadlocks::numeric AS database_deadlocks,
        blk_read_time::numeric AS database_blk_read_time_ms,
        blk_write_time::numeric AS database_blk_write_time_ms,
        stats_reset AS database_stats_reset
    FROM pg_stat_database
    WHERE datid = (SELECT oid FROM pg_database WHERE datname = current_database())
),
client_io_snapshot AS (
    SELECT
        coalesce(sum(reads), 0)::numeric AS client_io_reads,
        coalesce(sum(writes), 0)::numeric AS client_io_writes,
        coalesce(sum(read_time), 0)::numeric AS client_io_read_time_ms,
        coalesce(sum(write_time), 0)::numeric AS client_io_write_time_ms,
        max(stats_reset) AS client_io_stats_reset
    FROM pg_stat_io
    WHERE backend_type = 'client backend'
),
checkpoint_snapshot AS (
    SELECT
        num_timed::numeric AS checkpoints_timed,
        num_requested::numeric AS checkpoints_requested,
        write_time::numeric AS checkpoint_write_time_ms,
        sync_time::numeric AS checkpoint_sync_time_ms,
        stats_reset AS checkpointer_stats_reset
    FROM pg_stat_checkpointer
),
resource_inputs AS (
    SELECT
        connection_limits.max_connections,
        greatest(
            0,
            connection_limits.max_connections
            - connection_limits.reserved_connections
            - connection_limits.superuser_reserved_connections
        ) AS connection_capacity,
        connection_limits.track_io_timing,
        session_snapshot.client_backends,
        session_snapshot.active_client_backends,
        session_snapshot.waiting_client_backends,
        session_snapshot.idle_in_transaction_backends,
        database_snapshot.database_blks_read,
        database_snapshot.database_blks_hit,
        database_snapshot.database_temp_bytes,
        database_snapshot.database_deadlocks,
        database_snapshot.database_blk_read_time_ms,
        database_snapshot.database_blk_write_time_ms,
        database_snapshot.database_stats_reset,
        client_io_snapshot.client_io_reads,
        client_io_snapshot.client_io_writes,
        client_io_snapshot.client_io_read_time_ms,
        client_io_snapshot.client_io_write_time_ms,
        client_io_snapshot.client_io_stats_reset,
        checkpoint_snapshot.checkpoints_timed,
        checkpoint_snapshot.checkpoints_requested,
        checkpoint_snapshot.checkpoint_write_time_ms,
        checkpoint_snapshot.checkpoint_sync_time_ms,
        checkpoint_snapshot.checkpointer_stats_reset
    FROM connection_limits
    CROSS JOIN session_snapshot
    CROSS JOIN database_snapshot
    CROSS JOIN client_io_snapshot
    CROSS JOIN checkpoint_snapshot
)
SELECT
    CASE
        WHEN connection_capacity <= 0 THEN 'UNKNOWN_CONNECTION_CAPACITY'
        WHEN client_backends >= connection_capacity THEN 'SATURATED_CONNECTIONS'
        WHEN 100 * client_backends / NULLIF(connection_capacity, 0) >= 85
             AND waiting_client_backends > 0 THEN 'HIGH_CONNECTION_PRESSURE'
        WHEN 100 * client_backends / NULLIF(connection_capacity, 0) >= 85
             THEN 'HIGH_CONNECTION_UTILIZATION'
        ELSE 'NO_CONNECTION_PRESSURE_SIGNAL'
    END AS resource_pressure_status,
    max_connections,
    connection_capacity,
    client_backends,
    active_client_backends,
    waiting_client_backends,
    idle_in_transaction_backends,
    round(100 * client_backends / NULLIF(connection_capacity, 0), 2) AS connection_utilization_pct,
    round(100 * waiting_client_backends / NULLIF(active_client_backends, 0), 2) AS waiting_active_pct,
    track_io_timing,
    round(100 * database_blks_hit / NULLIF(database_blks_hit + database_blks_read, 0), 2)
        AS database_cache_hit_pct,
    database_blks_read,
    database_blks_hit,
    database_temp_bytes,
    database_deadlocks,
    database_blk_read_time_ms,
    database_blk_write_time_ms,
    client_io_reads,
    client_io_writes,
    client_io_read_time_ms,
    client_io_write_time_ms,
    checkpoints_timed,
    checkpoints_requested,
    round(
        100 * checkpoints_requested
        / NULLIF(checkpoints_timed + checkpoints_requested, 0),
        2
    ) AS checkpoint_requested_pct,
    checkpoint_write_time_ms,
    checkpoint_sync_time_ms,
    database_stats_reset,
    client_io_stats_reset,
    checkpointer_stats_reset
FROM resource_inputs;

-- Phase 1 advisory assessment. Cumulative counters are useful only when the
-- preceding capture shares all reset timestamps, so invalid windows are never
-- treated as evidence of pressure or scaling demand.
CREATE TEMP TABLE :"pss_advisor_table" ON COMMIT DROP AS
WITH configuration AS (
    SELECT
        current_setting('citus_advisor.pressure_samples', true)::integer AS pressure_samples,
        current_setting('citus_advisor.pressure_window_minutes', true)::integer
            AS pressure_window_minutes,
        current_setting('citus_advisor.min_window_seconds', true)::integer
            AS min_window_seconds,
        coalesce(current_setting('citus_advisor.wait_sampling_available', true)::boolean, false)
            AS wait_sampling_available,
        coalesce(current_setting('citus_advisor.citus_statements_available', true)::boolean, false)
            AS citus_statements_available,
        coalesce(current_setting('pg_stat_statements.track_planning', true)::boolean, false)
            AS planning_tracked,
        EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus') AS is_citus,
        EXISTS (
            SELECT 1
            FROM :"pss_fairness_table"
            WHERE table_name = '(cluster overall)'
        ) AS fairness_available
),
current_snapshot AS (
    SELECT
        current_setting('pss.run_tag', true) AS capture_key,
        now() AS collected_at,
        current_setting('citus_advisor.server_id', true) AS server_id,
        current_setting('citus_advisor.database_oid', true)::integer AS database_oid,
        current_setting('citus_advisor.database_name', true) AS database_name,
        (SELECT stats_reset FROM public.pg_stat_statements_info) AS pgss_stats_reset,
        coalesce((SELECT sum(calls) FROM :"pss_features_table"), 0)::numeric
            AS workload_calls_total,
        coalesce((SELECT sum(total_exec_ms) FROM :"pss_features_table"), 0)::numeric
            AS workload_exec_ms_total,
        coalesce((SELECT sum(io_weight) FROM :"pss_features_table"), 0)::numeric
            AS workload_io_blocks_total,
        resource.*
    FROM :"pss_resource_table" AS resource
),
previous AS (
    SELECT
        current_snapshot.*,
        configuration.*,
        prior.collected_at AS prior_collected_at,
        prior.pgss_stats_reset AS prior_pgss_stats_reset,
        prior.database_stats_reset AS prior_database_stats_reset,
        prior.client_io_stats_reset AS prior_client_io_stats_reset,
        prior.checkpointer_stats_reset AS prior_checkpointer_stats_reset,
        prior.workload_calls_total AS prior_workload_calls_total,
        prior.workload_exec_ms_total AS prior_workload_exec_ms_total,
        prior.workload_io_blocks_total AS prior_workload_io_blocks_total,
        prior.database_temp_bytes AS prior_database_temp_bytes,
        prior.database_deadlocks AS prior_database_deadlocks,
        prior.client_io_reads AS prior_client_io_reads,
        prior.client_io_writes AS prior_client_io_writes,
        prior.client_io_read_time_ms AS prior_client_io_read_time_ms,
        prior.client_io_write_time_ms AS prior_client_io_write_time_ms,
        prior.checkpoints_timed AS prior_checkpoints_timed,
        prior.checkpoints_requested AS prior_checkpoints_requested,
        prior.checkpoint_write_time_ms AS prior_checkpoint_write_time_ms,
        prior.checkpoint_sync_time_ms AS prior_checkpoint_sync_time_ms
    FROM current_snapshot
    CROSS JOIN configuration
    LEFT JOIN LATERAL (
        SELECT history.*
        FROM citus_advisor.capture_history AS history
        WHERE history.server_id = current_snapshot.server_id
          AND history.database_oid = current_snapshot.database_oid
        ORDER BY history.collected_at DESC
        LIMIT 1
    ) AS prior ON true
),
window_checks AS (
    SELECT
        previous.*,
        extract(epoch FROM collected_at - prior_collected_at)::numeric AS window_seconds,
        CASE
            WHEN prior_collected_at IS NULL THEN 'NO_PRIOR_CAPTURE'
            WHEN pgss_stats_reset IS DISTINCT FROM prior_pgss_stats_reset
              OR database_stats_reset IS DISTINCT FROM prior_database_stats_reset
              OR client_io_stats_reset IS DISTINCT FROM prior_client_io_stats_reset
              OR checkpointer_stats_reset IS DISTINCT FROM prior_checkpointer_stats_reset
                THEN 'STATISTICS_RESET'
            WHEN extract(epoch FROM collected_at - prior_collected_at) < min_window_seconds
                THEN 'WINDOW_TOO_SHORT'
            WHEN workload_calls_total < prior_workload_calls_total
              OR workload_exec_ms_total < prior_workload_exec_ms_total
              OR workload_io_blocks_total < prior_workload_io_blocks_total
              OR database_temp_bytes < prior_database_temp_bytes
              OR database_deadlocks < prior_database_deadlocks
              OR client_io_reads < prior_client_io_reads
              OR client_io_writes < prior_client_io_writes
              OR checkpoints_timed < prior_checkpoints_timed
              OR checkpoints_requested < prior_checkpoints_requested
                THEN 'COUNTER_REGRESSION'
            ELSE NULL
        END AS window_blocker
    FROM previous
),
delta AS (
    SELECT
        window_checks.*,
        window_blocker IS NULL AS window_valid,
        CASE WHEN window_blocker IS NULL
            THEN workload_calls_total - prior_workload_calls_total
        END AS workload_calls_delta,
        CASE WHEN window_blocker IS NULL
            THEN workload_exec_ms_total - prior_workload_exec_ms_total
        END AS workload_exec_ms_delta,
        CASE WHEN window_blocker IS NULL
            THEN workload_io_blocks_total - prior_workload_io_blocks_total
        END AS workload_io_blocks_delta,
        CASE WHEN window_blocker IS NULL
            THEN database_temp_bytes - prior_database_temp_bytes
        END AS database_temp_bytes_delta,
        CASE WHEN window_blocker IS NULL
            THEN database_deadlocks - prior_database_deadlocks
        END AS database_deadlocks_delta,
        CASE WHEN window_blocker IS NULL
            THEN client_io_reads - prior_client_io_reads
        END AS client_io_reads_delta,
        CASE WHEN window_blocker IS NULL
            THEN client_io_writes - prior_client_io_writes
        END AS client_io_writes_delta,
        CASE WHEN window_blocker IS NULL
            THEN checkpoints_requested - prior_checkpoints_requested
        END AS checkpoints_requested_delta
    FROM window_checks
),
recent_history AS (
    SELECT
        history.resource_pressure_status,
        history.collected_at
    FROM citus_advisor.capture_history AS history
    CROSS JOIN delta
    WHERE history.server_id = delta.server_id
      AND history.database_oid = delta.database_oid
      AND history.pgss_stats_reset IS NOT DISTINCT FROM delta.pgss_stats_reset
      AND history.database_stats_reset IS NOT DISTINCT FROM delta.database_stats_reset
      AND history.client_io_stats_reset IS NOT DISTINCT FROM delta.client_io_stats_reset
      AND history.checkpointer_stats_reset IS NOT DISTINCT FROM delta.checkpointer_stats_reset
      AND history.collected_at >= delta.collected_at
          - make_interval(mins => delta.pressure_window_minutes)
    ORDER BY history.collected_at DESC
    LIMIT (SELECT greatest(pressure_samples - 1, 0) FROM delta)
),
pressure_observations AS (
    SELECT resource_pressure_status, collected_at
    FROM recent_history

    UNION ALL

    SELECT resource_pressure_status, collected_at
    FROM delta
),
pressure_summary AS (
    SELECT
        count(*)::integer AS pressure_capture_count,
        count(*) FILTER (
            WHERE resource_pressure_status IN (
                'SATURATED_CONNECTIONS',
                'HIGH_CONNECTION_PRESSURE',
                'HIGH_CONNECTION_UTILIZATION'
            )
        )::integer AS pressured_capture_count
    FROM pressure_observations
),
calibration AS (
    SELECT
        count(feedback.feedback_id)::integer AS calibration_feedback_count,
        round(
            100.0
            * count(feedback.feedback_id) FILTER (
                WHERE feedback.outcome IN ('success', 'improved', 'confirmed_benefit')
            )
            / NULLIF(count(feedback.feedback_id), 0),
            2
        ) AS calibration_positive_outcome_pct
    FROM citus_advisor.recommendation_feedback AS feedback
    JOIN citus_advisor.capture_history AS history
      ON history.capture_key = feedback.capture_key
    CROSS JOIN delta
    WHERE history.server_id = delta.server_id
      AND history.database_oid = delta.database_oid
),
coverage AS (
    SELECT
        delta.*,
        pressure_summary.pressure_capture_count,
        pressure_summary.pressured_capture_count,
        calibration.calibration_feedback_count,
        calibration.calibration_positive_outcome_pct,
        (
            30
            + CASE WHEN window_valid THEN 30 ELSE 0 END
            + CASE WHEN track_io_timing THEN 10 ELSE 0 END
            + CASE WHEN wait_sampling_available THEN 10 ELSE 0 END
            + CASE
                WHEN NOT is_citus
                  OR (fairness_available AND citus_statements_available)
                    THEN 20
                ELSE 0
              END
        )::numeric AS telemetry_coverage_pct
    FROM delta
    CROSS JOIN pressure_summary
    CROSS JOIN calibration
),
assessment AS (
    SELECT
        coverage.*,
        (
            window_valid
            AND pressure_capture_count >= pressure_samples
            AND pressured_capture_count >= pressure_samples
        ) AS sustained_resource_pressure,
        CASE
            WHEN window_valid
             AND pressure_capture_count >= pressure_samples
             AND telemetry_coverage_pct >= 80 THEN 'HIGH'
            WHEN window_valid
             AND telemetry_coverage_pct >= 60 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS recommendation_confidence
    FROM coverage
)
SELECT
    capture_key,
    collected_at,
    server_id,
    database_oid,
    database_name,
    pgss_stats_reset,
    database_stats_reset,
    client_io_stats_reset,
    checkpointer_stats_reset,
    workload_calls_total,
    workload_exec_ms_total,
    workload_io_blocks_total,
    resource_pressure_status,
    connection_utilization_pct,
    waiting_active_pct,
    database_temp_bytes,
    database_deadlocks,
    client_io_reads,
    client_io_writes,
    client_io_read_time_ms,
    client_io_write_time_ms,
    checkpoints_timed,
    checkpoints_requested,
    checkpoint_write_time_ms,
    checkpoint_sync_time_ms,
    window_valid,
    window_blocker,
    window_seconds,
    workload_calls_delta,
    workload_exec_ms_delta,
    workload_io_blocks_delta,
    database_temp_bytes_delta,
    database_deadlocks_delta,
    client_io_reads_delta,
    client_io_writes_delta,
    checkpoints_requested_delta,
    round(workload_calls_delta / NULLIF(window_seconds, 0), 4) AS workload_calls_per_second,
    round(workload_exec_ms_delta / NULLIF(window_seconds, 0), 4) AS workload_exec_ms_per_second,
    round(workload_io_blocks_delta / NULLIF(window_seconds, 0), 4) AS workload_io_blocks_per_second,
    round(database_temp_bytes_delta / NULLIF(window_seconds, 0), 4) AS temp_bytes_per_second,
    round(database_deadlocks_delta * 3600 / NULLIF(window_seconds, 0), 4) AS deadlocks_per_hour,
    round(client_io_reads_delta / NULLIF(window_seconds, 0), 4) AS client_io_reads_per_second,
    round(client_io_writes_delta / NULLIF(window_seconds, 0), 4) AS client_io_writes_per_second,
    round(checkpoints_requested_delta / NULLIF(window_seconds, 0), 4)
        AS checkpoints_requested_per_second,
    pressure_samples,
    pressure_capture_count,
    pressured_capture_count,
    sustained_resource_pressure,
    telemetry_coverage_pct,
    recommendation_confidence,
    calibration_feedback_count,
    calibration_positive_outcome_pct,
    concat_ws(
        '; ',
        CASE WHEN window_blocker IS NOT NULL THEN lower(replace(window_blocker, '_', ' ')) END,
        CASE WHEN pressure_capture_count < pressure_samples
            THEN format('need %s captures in %s minutes', pressure_samples, pressure_window_minutes)
        END,
        CASE WHEN NOT sustained_resource_pressure THEN 'resource pressure not sustained' END,
        CASE WHEN NOT track_io_timing THEN 'track_io_timing is off' END,
        CASE WHEN NOT wait_sampling_available THEN 'wait sampling unavailable' END,
        CASE WHEN is_citus AND NOT fairness_available THEN 'Citus fairness unavailable' END,
        CASE WHEN is_citus AND NOT citus_statements_available THEN 'Citus routing telemetry unavailable' END
    ) AS blocking_signals,
    concat_ws(
        '; ',
        format('window=%s seconds', coalesce(round(window_seconds, 1)::text, 'n/a')),
        format('pressure captures=%s/%s', pressured_capture_count, pressure_samples),
        format('connection=%s%%', coalesce(connection_utilization_pct::text, 'n/a')),
        format('waiting active=%s%%', coalesce(waiting_active_pct::text, 'n/a')),
        format('workload exec=%s ms/s', coalesce(round(workload_exec_ms_delta / NULLIF(window_seconds, 0), 2)::text, 'n/a'))
    ) AS evidence_summary
FROM assessment;

\if :verbose
-- Result set 7: per-query routability (worst first).
SELECT
    queryid,
    calls,
    round(total_exec_ms, 2)                       AS total_exec_ms,
    is_cross_node,
    routability_score,
    left(regexp_replace(query, E'\\s+', ' ', 'g'), 220) AS query_sample
FROM :"pss_locality_table"
ORDER BY routability_score ASC, total_exec_ms DESC;

-- Result set 8: execution-time-weighted routability summary.
SELECT *
FROM :"pss_locality_summary_table";
\endif

-- Result set 9: the whole score in a single final row - execution-time weighted
-- workload mix, Citus data distribution fairness, and routability headline
-- metrics combined for an at-a-glance summary. It is materialized into
-- a generated final-score temp table (with a collected_at timestamp) so the same row can be both
-- displayed and appended to the Grafana CSV below.
CREATE TEMP TABLE :"pss_final_table" ON COMMIT DROP AS
WITH scores AS (
    SELECT
        sum(oltp_raw       * exec_weight) AS oltp_w,
        sum(olap_raw       * exec_weight) AS olap_w,
        sum(htap_raw       * exec_weight) AS htap_w,
        sum(timeseries_raw * exec_weight) AS timeseries_w
    FROM :"pss_features_table"
),
norm AS (
    SELECT
        *,
        (oltp_w + olap_w + htap_w + timeseries_w) AS total_w
    FROM scores
),
locality AS (
    SELECT *
    FROM :"pss_locality_summary_table"
),
platform AS (
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus') AS is_citus
),
resource AS (
    SELECT *
    FROM :"pss_resource_table"
),
base AS (
    SELECT
        now()                                                      AS collected_at,
        round(100 * norm.oltp_w       / NULLIF(norm.total_w, 0), 2) AS oltp_pct,
        round(100 * norm.olap_w       / NULLIF(norm.total_w, 0), 2) AS olap_pct,
        round(100 * norm.htap_w       / NULLIF(norm.total_w, 0), 2) AS htap_pct,
        round(100 * norm.timeseries_w / NULLIF(norm.total_w, 0), 2) AS timeseries_pct,
        (SELECT data_distribution_fairness
           FROM :"pss_fairness_table"
          WHERE table_name = '(cluster overall)')                  AS data_distribution_fairness,
        locality.ideal_local_exec_pct,
        locality.cross_node_exec_pct,
        locality.mean_routability_score,
        platform.is_citus
    FROM norm CROSS JOIN locality CROSS JOIN platform
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
    base.is_citus,
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
        WHEN COALESCE(base.olap_pct, 0) + COALESCE(base.htap_pct, 0) >= 40 AND base.is_citus
            THEN 'SCALE_OUT: analytical/mixed demand with good routability and balanced shards; adding worker nodes should scale near-linearly.'
        WHEN COALESCE(base.olap_pct, 0) + COALESCE(base.htap_pct, 0) >= 40
            THEN 'SCALE_OUT: analytical/mixed demand is a candidate to move to Citus / Azure Elastic Clusters for horizontal scale-out.'
        WHEN COALESCE(base.oltp_pct, 0) >= 50 AND COALESCE(base.ideal_local_exec_pct, 0) >= 60 AND base.is_citus
            THEN 'SCALE_OUT: routable OLTP throughput; single-shard queries spread cleanly across nodes (Citus MX).'
        WHEN COALESCE(base.oltp_pct, 0) >= 50 AND COALESCE(base.ideal_local_exec_pct, 0) >= 60
            THEN 'SCALE_OUT: routable OLTP throughput is a candidate to move to Citus / Azure Elastic Clusters for horizontal scale-out.'
        WHEN COALESCE(base.oltp_pct, 0) >= 50
            THEN 'SCALE_UP_OR_TUNE: predominantly OLTP; prefer vertical scale / pooling / indexing unless throughput-bound, then shard by tenant key.'
        ELSE 'REVIEW: mixed signals; inspect the detailed result sets (run with -v verbose=on).'
    END                                                            AS scale_recommendation
    , resource.resource_pressure_status
    , resource.max_connections
    , resource.connection_capacity
    , resource.client_backends
    , resource.active_client_backends
    , resource.waiting_client_backends
    , resource.idle_in_transaction_backends
    , resource.connection_utilization_pct
    , resource.waiting_active_pct
    , resource.track_io_timing
    , resource.database_cache_hit_pct
    , resource.database_blks_read
    , resource.database_blks_hit
    , resource.database_temp_bytes
    , resource.database_deadlocks
    , resource.database_blk_read_time_ms
    , resource.database_blk_write_time_ms
    , resource.client_io_reads
    , resource.client_io_writes
    , resource.client_io_read_time_ms
    , resource.client_io_write_time_ms
    , resource.checkpoints_timed
    , resource.checkpoints_requested
    , resource.checkpoint_requested_pct
    , resource.checkpoint_write_time_ms
    , resource.checkpoint_sync_time_ms
    , resource.database_stats_reset
    , resource.client_io_stats_reset
    , resource.checkpointer_stats_reset
FROM base
CROSS JOIN resource;

-- The original score remains as workload_fit_recommendation. The Phase 1
-- action below is the operator-facing decision and cannot become a scale
-- candidate until reset-safe, sustained pressure is present.
CREATE TEMP TABLE :"pss_output_table" ON COMMIT DROP AS
WITH decision AS (
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
        base.is_citus AS citus_installed,
        base.dominant_workload,
        base.scale_recommendation AS workload_fit_recommendation,
        base.resource_pressure_status,
        base.max_connections,
        base.connection_capacity,
        base.client_backends,
        base.active_client_backends,
        base.waiting_client_backends,
        base.idle_in_transaction_backends,
        base.connection_utilization_pct,
        base.waiting_active_pct,
        base.track_io_timing,
        base.database_cache_hit_pct,
        base.database_blks_read,
        base.database_blks_hit,
        base.database_temp_bytes,
        base.database_deadlocks,
        base.database_blk_read_time_ms,
        base.database_blk_write_time_ms,
        base.client_io_reads,
        base.client_io_writes,
        base.client_io_read_time_ms,
        base.client_io_write_time_ms,
        base.checkpoints_timed,
        base.checkpoints_requested,
        base.checkpoint_requested_pct,
        base.checkpoint_write_time_ms,
        base.checkpoint_sync_time_ms,
        base.database_stats_reset,
        base.client_io_stats_reset,
        base.checkpointer_stats_reset,
        advisor.capture_key,
        advisor.server_id,
        advisor.database_oid,
        advisor.database_name,
        advisor.pgss_stats_reset,
        advisor.workload_calls_total,
        advisor.workload_exec_ms_total,
        advisor.workload_io_blocks_total,
        advisor.window_valid,
        advisor.window_blocker,
        advisor.window_seconds,
        advisor.workload_calls_delta,
        advisor.workload_exec_ms_delta,
        advisor.workload_io_blocks_delta,
        advisor.database_temp_bytes_delta,
        advisor.database_deadlocks_delta,
        advisor.client_io_reads_delta,
        advisor.client_io_writes_delta,
        advisor.checkpoints_requested_delta,
        advisor.workload_calls_per_second,
        advisor.workload_exec_ms_per_second,
        advisor.workload_io_blocks_per_second,
        advisor.temp_bytes_per_second,
        advisor.deadlocks_per_hour,
        advisor.client_io_reads_per_second,
        advisor.client_io_writes_per_second,
        advisor.checkpoints_requested_per_second,
        advisor.pressure_samples,
        advisor.pressure_capture_count,
        advisor.pressured_capture_count,
        advisor.sustained_resource_pressure,
        advisor.telemetry_coverage_pct,
        advisor.recommendation_confidence,
        advisor.calibration_feedback_count,
        advisor.calibration_positive_outcome_pct,
        advisor.blocking_signals,
        advisor.evidence_summary,
        CASE
            WHEN COALESCE(base.oltp_pct, 0) + COALESCE(base.olap_pct, 0)
               + COALESCE(base.htap_pct, 0) + COALESCE(base.timeseries_pct, 0) = 0
                THEN 'REVIEW_TELEMETRY'
            WHEN base.data_distribution_fairness IS NOT NULL
             AND base.data_distribution_fairness < 0.85
                THEN 'REBALANCE_FIRST'
            WHEN COALESCE(base.cross_node_exec_pct, 0) >= 40
              OR COALESCE(base.mean_routability_score, 1) < 0.60
                THEN 'OPTIMIZE_LOCALITY_FIRST'
            WHEN advisor.recommendation_confidence = 'LOW'
                THEN 'REVIEW_TELEMETRY'
            WHEN NOT advisor.sustained_resource_pressure
                THEN 'MONITOR'
            WHEN base.resource_pressure_status IN (
                'SATURATED_CONNECTIONS',
                'HIGH_CONNECTION_PRESSURE',
                'HIGH_CONNECTION_UTILIZATION'
            ) AND COALESCE(base.oltp_pct, 0) >= 50
                THEN 'ADD_CONNECTION_POOLING'
            WHEN COALESCE(base.olap_pct, 0) + COALESCE(base.htap_pct, 0) >= 40
             AND base.is_citus
                THEN 'ADD_CITUS_WORKERS_CANDIDATE'
            WHEN COALESCE(base.olap_pct, 0) + COALESCE(base.htap_pct, 0) >= 40
                THEN 'CITUS_MIGRATION_CANDIDATE'
            WHEN COALESCE(base.oltp_pct, 0) >= 50 AND base.is_citus
                THEN 'ADD_CITUS_WORKERS_CANDIDATE'
            WHEN COALESCE(base.oltp_pct, 0) >= 50
                THEN 'CITUS_MIGRATION_CANDIDATE'
            WHEN advisor.sustained_resource_pressure
                THEN 'SCALE_UP_CANDIDATE'
            ELSE 'TUNE_QUERY_OR_INDEX'
        END AS recommended_action
    FROM :"pss_final_table" AS base
    CROSS JOIN :"pss_advisor_table" AS advisor
),
recommendation AS (
    SELECT
        decision.*,
        CASE recommended_action
            WHEN 'REBALANCE_FIRST'
                THEN 'REBALANCE_FIRST: shard-byte fairness is below the safe scale-out threshold.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST'
                THEN 'OPTIMIZE_LOCALITY_FIRST: cross-node execution or routability blocks efficient scaling.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE'
                THEN 'SCALE_OUT: sustained pressure and workload fit support adding Citus worker capacity.'
            WHEN 'CITUS_MIGRATION_CANDIDATE'
                THEN 'SCALE_OUT: sustained pressure and workload fit support evaluating a move to Citus / Azure Elastic Clusters.'
            WHEN 'ADD_CONNECTION_POOLING'
                THEN 'SCALE_UP_OR_TUNE: sustained connection pressure favors pooling before adding database capacity.'
            WHEN 'SCALE_UP_CANDIDATE'
                THEN 'SCALE_UP_OR_TUNE: sustained pressure lacks a strong distributed-scale fit.'
            WHEN 'MONITOR'
                THEN 'REVIEW: workload fit exists, but resource pressure is not sustained across the required capture window.'
            WHEN 'TUNE_QUERY_OR_INDEX'
                THEN 'REVIEW: sustained pressure exists without a clear horizontal-scale fit; inspect query and index evidence.'
            ELSE 'REVIEW: insufficient reset-safe telemetry for an operator action.'
        END AS scale_recommendation,
        CASE recommended_action
            WHEN 'REBALANCE_FIRST'
                THEN 'Restore even shard-byte distribution so additional workers can absorb load predictably.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST'
                THEN 'Reduce scatter/gather work and improve the expected benefit of horizontal capacity.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE'
                THEN 'Increase distributed execution capacity for sustained analytical, mixed, or routable OLTP demand.'
            WHEN 'CITUS_MIGRATION_CANDIDATE'
                THEN 'Evaluate horizontal capacity after a Citus distribution design review.'
            WHEN 'ADD_CONNECTION_POOLING'
                THEN 'Lower backend connection pressure without immediately increasing database compute.'
            WHEN 'SCALE_UP_CANDIDATE'
                THEN 'Increase single-node compute or storage capacity for sustained pressure.'
            WHEN 'MONITOR'
                THEN 'Avoid unnecessary scaling until sustained pressure is demonstrated.'
            WHEN 'TUNE_QUERY_OR_INDEX'
                THEN 'Reduce inefficient work before committing to additional capacity.'
            ELSE 'Collect a reset-safe capture window with sufficient telemetry coverage.'
        END AS expected_benefit,
        CASE recommended_action
            WHEN 'REBALANCE_FIRST'
                THEN 'Re-run after rebalance and confirm fairness, locality, and pressure for three captures.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST'
                THEN 'Re-run after distribution or co-location changes and confirm lower cross-node execution.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE'
                THEN 'After adding workers, compare pressure, latency, and routability across the next three captures.'
            WHEN 'CITUS_MIGRATION_CANDIDATE'
                THEN 'Complete the Phase 2 Citus readiness assessment before migration.'
            WHEN 'ADD_CONNECTION_POOLING'
                THEN 'Apply pooling and confirm reduced connection utilization and waiting-session share.'
            WHEN 'SCALE_UP_CANDIDATE'
                THEN 'After scaling up, confirm lower resource pressure across three reset-safe captures.'
            WHEN 'MONITOR'
                THEN 'Collect the remaining captures inside the configured pressure window.'
            WHEN 'TUNE_QUERY_OR_INDEX'
                THEN 'Inspect the highest execution-time fingerprints and validate the change with new captures.'
            ELSE 'Collect a valid baseline and enough capture intervals to establish confidence.'
        END AS verification_step
    FROM decision
)
SELECT *
FROM recommendation;

INSERT INTO citus_advisor.capture_history (
    capture_key,
    collected_at,
    server_id,
    database_oid,
    database_name,
    pgss_stats_reset,
    database_stats_reset,
    client_io_stats_reset,
    checkpointer_stats_reset,
    workload_calls_total,
    workload_exec_ms_total,
    workload_io_blocks_total,
    resource_pressure_status,
    connection_utilization_pct,
    waiting_active_pct,
    database_temp_bytes,
    database_deadlocks,
    client_io_reads,
    client_io_writes,
    client_io_read_time_ms,
    client_io_write_time_ms,
    checkpoints_timed,
    checkpoints_requested,
    checkpoint_write_time_ms,
    checkpoint_sync_time_ms,
    recommended_action,
    recommendation_confidence,
    telemetry_coverage_pct,
    blocking_signals,
    evidence_summary,
    expected_benefit,
    verification_step
)
SELECT
    capture_key,
    collected_at,
    server_id,
    database_oid,
    database_name,
    pgss_stats_reset,
    database_stats_reset,
    client_io_stats_reset,
    checkpointer_stats_reset,
    workload_calls_total,
    workload_exec_ms_total,
    workload_io_blocks_total,
    resource_pressure_status,
    connection_utilization_pct,
    waiting_active_pct,
    database_temp_bytes,
    database_deadlocks,
    client_io_reads,
    client_io_writes,
    client_io_read_time_ms,
    client_io_write_time_ms,
    checkpoints_timed,
    checkpoints_requested,
    checkpoint_write_time_ms,
    checkpoint_sync_time_ms,
    recommended_action,
    recommendation_confidence,
    telemetry_coverage_pct,
    blocking_signals,
    evidence_summary,
    expected_benefit,
    verification_step
FROM :"pss_output_table"
ON CONFLICT (capture_key) DO NOTHING;

SET LOCAL pss.final_table = :'pss_output_table';

SELECT *
FROM :"pss_output_table";

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
    v_table_name text := current_setting('pss.final_table', true);
    v_hdr text;
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

        SELECT string_agg(attribute.attname, ',' ORDER BY attribute.attnum)
        INTO v_hdr
        FROM pg_attribute AS attribute
        WHERE attribute.attrelid = to_regclass(v_table_name)
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped;

        IF v_hdr IS NULL THEN
                RAISE EXCEPTION 'Could not determine CSV columns for advisor output table %', v_table_name;
        END IF;

    -- Shell: create the header once (when the file is missing or empty), then
    -- append the CSV rows COPY streams in on stdin.
    v_prog := format($p$f="%s"; test -s "$f" || printf '%%s\n' '%s' > "$f"; cat >> "$f"$p$, v_path, v_hdr);

    BEGIN
        EXECUTE format('COPY %I TO PROGRAM %L WITH (FORMAT csv)', v_table_name, v_prog);
        RAISE NOTICE 'Workload score appended to CSV for Grafana: %', v_abs_path;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'CSV export failed (%); COPY ... TO PROGRAM needs superuser or pg_execute_server_program. Path: %',
            SQLERRM, v_abs_path;
    END;
END
$plpgsql$;

COMMIT;
