-- workload_score_without_pss.sql
--
-- Purpose
--   Build a coarse PostgreSQL workload classifier using only core stats views,
--   while opportunistically incorporating pg_stat_statements signals when the
--   extension is installed.
--
-- Output
--   1) A feature snapshot from core views
--   2) Category scores normalized to percentages for:
--        OLTP, OLAP, HTAP, TIME_SERIES
--   3) If pg_stat_statements is installed, a second result set with bonus signals
--      extracted from query fingerprints
--
-- Notes
--   * This is best run over a meaningful observation window. Consider resetting
--     stats, running your workload for N minutes, then running this script.
--   * The scoring is heuristic. It is meant to guide benchmarking, not to act as
--     a formal taxonomy.
--   * TIME_SERIES here is treated as an additional pattern, not as a mutually
--     exclusive class.
--
-- Tested approach:
--   psql -f workload_score_without_pss.sql

\pset tuples_only off
\pset pager off

SET client_min_messages = warning;

BEGIN;

DROP TABLE IF EXISTS _wrk_core_features;
CREATE TEMP TABLE _wrk_core_features AS
WITH db AS (
    SELECT *
    FROM pg_stat_database
    WHERE datname = current_database()
),
activity AS (
    SELECT
        count(*) FILTER (WHERE state = 'active')::numeric                              AS active_sessions,
        count(*) FILTER (WHERE wait_event_type IS NOT NULL)::numeric                   AS waiting_sessions,
        count(*) FILTER (
            WHERE state = 'active'
              AND xact_start IS NOT NULL
              AND now() - xact_start > interval '5 minutes'
        )::numeric                                                                     AS long_xacts,
        count(*) FILTER (
            WHERE state = 'active'
              AND query_start IS NOT NULL
              AND now() - query_start > interval '30 seconds'
        )::numeric                                                                     AS long_queries
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
),
tbl AS (
    SELECT
        coalesce(sum(seq_scan), 0)::numeric                                            AS seq_scan,
        coalesce(sum(idx_scan), 0)::numeric                                            AS idx_scan,
        coalesce(sum(n_tup_ins), 0)::numeric                                           AS n_tup_ins,
        coalesce(sum(n_tup_upd), 0)::numeric                                           AS n_tup_upd,
        coalesce(sum(n_tup_del), 0)::numeric                                           AS n_tup_del,
        coalesce(sum(n_live_tup), 0)::numeric                                          AS n_live_tup,
        coalesce(sum(n_dead_tup), 0)::numeric                                          AS n_dead_tup,
        coalesce(sum(CASE WHEN n_tup_ins > greatest(10, (n_tup_upd + n_tup_del) * 3)
                          THEN 1 ELSE 0 END), 0)::numeric                              AS append_heavy_tables,
        coalesce(sum(CASE WHEN seq_scan > idx_scan * 2 AND n_live_tup > 100000
                          THEN 1 ELSE 0 END), 0)::numeric                              AS scan_heavy_large_tables,
        coalesce(sum(CASE WHEN idx_scan > seq_scan * 2 AND n_live_tup > 1000
                          THEN 1 ELSE 0 END), 0)::numeric                              AS index_heavy_tables
    FROM pg_stat_user_tables
),
stio AS (
    SELECT
        coalesce(sum(heap_blks_read + idx_blks_read), 0)::numeric                      AS blocks_read,
        coalesce(sum(heap_blks_hit  + idx_blks_hit), 0)::numeric                       AS blocks_hit
    FROM pg_statio_user_tables
)
SELECT
    current_database()                                                                 AS datname,
    now()                                                                              AS captured_at,
    db.xact_commit::numeric,
    db.xact_rollback::numeric,
    db.tup_returned::numeric,
    db.tup_fetched::numeric,
    db.tup_inserted::numeric,
    db.tup_updated::numeric,
    db.tup_deleted::numeric,
    db.blks_read::numeric,
    db.blks_hit::numeric,
    db.temp_files::numeric,
    db.temp_bytes::numeric,
    db.deadlocks::numeric,
    db.sessions::numeric,
    db.session_time,
    db.active_time,
    db.idle_in_transaction_time,
    activity.active_sessions,
    activity.waiting_sessions,
    activity.long_xacts,
    activity.long_queries,
    tbl.seq_scan,
    tbl.idx_scan,
    tbl.n_tup_ins,
    tbl.n_tup_upd,
    tbl.n_tup_del,
    tbl.n_live_tup,
    tbl.n_dead_tup,
    tbl.append_heavy_tables,
    tbl.scan_heavy_large_tables,
    tbl.index_heavy_tables,
    stio.blocks_read,
    stio.blocks_hit,

    -- Derived ratios
    (db.tup_inserted + db.tup_updated + db.tup_deleted)
        / greatest(db.tup_returned + db.tup_fetched + db.tup_inserted + db.tup_updated + db.tup_deleted, 1)::numeric
        AS write_ratio,

    (db.tup_returned + db.tup_fetched)
        / greatest(db.tup_inserted + db.tup_updated + db.tup_deleted, 1)::numeric
        AS read_to_write_ratio,

    tbl.idx_scan / greatest(tbl.seq_scan + tbl.idx_scan, 1)::numeric
        AS index_scan_ratio,

    tbl.seq_scan / greatest(tbl.seq_scan + tbl.idx_scan, 1)::numeric
        AS seq_scan_ratio,

    db.temp_bytes / greatest(db.blks_read + db.blks_hit, 1)::numeric
        AS temp_to_buffer_ratio,

    tbl.append_heavy_tables / greatest((SELECT count(*) FROM pg_stat_user_tables), 1)::numeric
        AS append_table_ratio,

    tbl.scan_heavy_large_tables / greatest((SELECT count(*) FROM pg_stat_user_tables), 1)::numeric
        AS scan_heavy_table_ratio,

    tbl.index_heavy_tables / greatest((SELECT count(*) FROM pg_stat_user_tables), 1)::numeric
        AS index_heavy_table_ratio
FROM db, activity, tbl, stio;

DROP TABLE IF EXISTS _wrk_pss_bonus;
CREATE TEMP TABLE _wrk_pss_bonus (
    pss_installed           boolean,
    pss_query_count         numeric,
    pss_query_calls         numeric,
    pss_total_exec_ms       numeric,
    pss_mean_exec_ms        numeric,
    pss_oltp_call_ratio     numeric,
    pss_olap_time_ratio     numeric,
    pss_dml_call_ratio      numeric,
    pss_agg_call_ratio      numeric,
    pss_temp_io_ratio       numeric,
    pss_plan_exec_ratio     numeric
);

DO $plpgsql$
DECLARE
    v_installed boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'pg_stat_statements'
    ) INTO v_installed;

    IF v_installed THEN
        EXECUTE $sql$
            INSERT INTO _wrk_pss_bonus
            WITH s AS (
                SELECT
                    queryid,
                    calls::numeric,
                    total_exec_time::numeric                                   AS total_exec_ms,
                    COALESCE(mean_exec_time::numeric,
                             total_exec_time::numeric / NULLIF(calls, 0), 0)    AS mean_exec_ms,
                    rows::numeric,
                    (shared_blks_hit + shared_blks_read)::numeric               AS shared_blks_total,
                    (temp_blks_read + temp_blks_written)::numeric               AS temp_blks_total,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name   = 'pg_stat_statements'
                              AND column_name  = 'total_plan_time'
                        ) THEN total_plan_time::numeric
                        ELSE 0::numeric
                    END                                                         AS total_plan_ms,
                    query
                FROM pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            SELECT
                true                                                                                           AS pss_installed,
                count(*)::numeric                                                                               AS pss_query_count,
                coalesce(sum(calls), 0)::numeric                                                                 AS pss_query_calls,
                coalesce(sum(total_exec_ms), 0)::numeric                                                         AS pss_total_exec_ms,
                coalesce(sum(total_exec_ms) / NULLIF(sum(calls), 0), 0)::numeric                                AS pss_mean_exec_ms,
                coalesce(sum(calls) FILTER (WHERE mean_exec_ms < 20 AND rows / NULLIF(calls, 0) < 100), 0)
                    / greatest(sum(calls), 1)::numeric                                                          AS pss_oltp_call_ratio,
                coalesce(sum(total_exec_ms) FILTER (
                    WHERE mean_exec_ms > 200
                       OR rows / NULLIF(calls, 0) > 1000
                       OR query ~* '\m(group\s+by|distinct|order\s+by|having|over\s*\()\M'
                ), 0)
                    / greatest(sum(total_exec_ms), 1)::numeric                                                  AS pss_olap_time_ratio,
                coalesce(sum(calls) FILTER (WHERE query ~* '^\s*(insert|update|delete)\m'), 0)
                    / greatest(sum(calls), 1)::numeric                                                          AS pss_dml_call_ratio,
                coalesce(sum(calls) FILTER (
                    WHERE query ~* '\m(group\s+by|distinct|order\s+by|having|over\s*\()\M'
                ), 0)
                    / greatest(sum(calls), 1)::numeric                                                          AS pss_agg_call_ratio,
                coalesce(sum(temp_blks_total), 0)
                    / greatest(sum(shared_blks_total + temp_blks_total), 1)::numeric                            AS pss_temp_io_ratio,
                coalesce(sum(total_plan_ms), 0)
                    / greatest(sum(total_exec_ms), 1)::numeric                                                  AS pss_plan_exec_ratio
            FROM s
        $sql$;
    ELSE
        INSERT INTO _wrk_pss_bonus
        VALUES (false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    END IF;
END
$plpgsql$;

DROP TABLE IF EXISTS _wrk_score;
CREATE TEMP TABLE _wrk_score AS
WITH f AS (
    SELECT * FROM _wrk_core_features
),
p AS (
    SELECT * FROM _wrk_pss_bonus
),
raw AS (
    SELECT
        f.datname,
        f.captured_at,

        -- OLTP: many writes, index-driven access, high concurrency, short-query bias
        (
            2.5 * LEAST(f.write_ratio * 4, 1)
          + 2.0 * LEAST(f.index_scan_ratio * 1.2, 1)
          + 1.2 * LEAST(f.active_sessions / 32, 1)
          + 1.0 * LEAST(f.index_heavy_table_ratio * 2, 1)
          + 0.6 * LEAST((1 - f.temp_to_buffer_ratio) * 1.1, 1)
          + CASE WHEN p.pss_installed THEN 1.8 * p.pss_oltp_call_ratio ELSE 0 END
          + CASE WHEN p.pss_installed THEN 1.0 * p.pss_dml_call_ratio ELSE 0 END
        )::numeric(20,6) AS oltp_raw,

        -- OLAP: scan-heavy, temp-heavy, long queries, lower locality
        (
            2.5 * LEAST(f.seq_scan_ratio * 1.5, 1)
          + 1.8 * LEAST(f.scan_heavy_table_ratio * 3, 1)
          + 1.7 * LEAST(f.temp_to_buffer_ratio * 8, 1)
          + 1.0 * LEAST(f.long_queries / 8, 1)
          + 0.5 * LEAST(f.read_to_write_ratio / 100, 1)
          + CASE WHEN p.pss_installed THEN 2.2 * p.pss_olap_time_ratio ELSE 0 END
          + CASE WHEN p.pss_installed THEN 1.0 * p.pss_agg_call_ratio ELSE 0 END
          + CASE WHEN p.pss_installed THEN 0.8 * p.pss_temp_io_ratio ELSE 0 END
        )::numeric(20,6) AS olap_raw,

        -- Time-series: append-heavy + relatively modest updates/deletes + some scan/range behavior
        (
            2.6 * LEAST(f.append_table_ratio * 3, 1)
          + 2.0 * LEAST(
                f.n_tup_ins / greatest(f.n_tup_ins + f.n_tup_upd + f.n_tup_del, 1),
                1
            )
          + 0.8 * LEAST(f.scan_heavy_table_ratio * 2, 1)
          + 0.6 * LEAST(f.index_scan_ratio * 1.2, 1)
          + CASE WHEN p.pss_installed THEN 0.5 * p.pss_dml_call_ratio ELSE 0 END
          + CASE WHEN p.pss_installed THEN 0.3 * p.pss_agg_call_ratio ELSE 0 END
        )::numeric(20,6) AS timeseries_raw,

        -- HTAP emerges when both OLTP and OLAP signals are materially present.
        (
            1.5 * GREATEST(
                    LEAST(
                        (
                            2.5 * LEAST(f.write_ratio * 4, 1)
                          + 2.0 * LEAST(f.index_scan_ratio * 1.2, 1)
                          + CASE WHEN p.pss_installed THEN 1.8 * p.pss_oltp_call_ratio ELSE 0 END
                        ) / 6.3,
                        (
                            2.5 * LEAST(f.seq_scan_ratio * 1.5, 1)
                          + 1.7 * LEAST(f.temp_to_buffer_ratio * 8, 1)
                          + CASE WHEN p.pss_installed THEN 2.2 * p.pss_olap_time_ratio ELSE 0 END
                        ) / 6.4
                    ),
                    0
              )
          + 0.7 * LEAST(f.active_sessions / 32, 1)
          + 0.6 * LEAST(f.long_queries / 8, 1)
          + CASE WHEN p.pss_installed AND p.pss_plan_exec_ratio > 0.05 THEN 0.5 ELSE 0 END
        )::numeric(20,6) AS htap_raw,

        p.pss_installed,
        p.pss_query_count,
        p.pss_query_calls,
        p.pss_total_exec_ms,
        p.pss_mean_exec_ms,
        p.pss_plan_exec_ratio
    FROM f
    CROSS JOIN p
),
norm AS (
    SELECT
        *,
        (oltp_raw + olap_raw + htap_raw + timeseries_raw) AS total_raw
    FROM raw
)
SELECT
    datname,
    captured_at,
    oltp_raw,
    olap_raw,
    htap_raw,
    timeseries_raw,
    round(100 * oltp_raw       / NULLIF(total_raw, 0), 2) AS oltp_pct,
    round(100 * olap_raw       / NULLIF(total_raw, 0), 2) AS olap_pct,
    round(100 * htap_raw       / NULLIF(total_raw, 0), 2) AS htap_pct,
    round(100 * timeseries_raw / NULLIF(total_raw, 0), 2) AS timeseries_pct,
    pss_installed,
    pss_query_count,
    pss_query_calls,
    round(pss_total_exec_ms, 2)     AS pss_total_exec_ms,
    round(pss_mean_exec_ms, 4)      AS pss_mean_exec_ms,
    round(pss_plan_exec_ratio, 4)   AS pss_plan_exec_ratio
FROM norm;

-- Result set 1: core features used by the scorer
SELECT *
FROM _wrk_core_features;

-- Result set 2: optional pg_stat_statements bonus features
SELECT *
FROM _wrk_pss_bonus;

-- Result set 3: final category scores
SELECT *
FROM _wrk_score;

COMMIT;
