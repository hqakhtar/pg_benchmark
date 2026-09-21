\echo Use "CREATE EXTENSION fspg_ec_advisor" to load this file. \quit

DO $plpgsql$
BEGIN
    IF current_setting('server_version_num')::integer < 170000 THEN
        RAISE EXCEPTION 'fspg_ec_advisor requires PostgreSQL 17 or later';
    END IF;
END
$plpgsql$;

CREATE TABLE fspg_ec_advisor.capture_history (
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
    database_temp_bytes            numeric,
    database_deadlocks             numeric,
    client_io_reads                numeric,
    client_io_writes               numeric,
    checkpoints_timed              numeric,
    checkpoints_requested          numeric,
    resource_pressure_status       text NOT NULL,
    recommended_action             text NOT NULL,
    recommendation_confidence      text NOT NULL,
    telemetry_coverage_pct         numeric NOT NULL,
    payload                        jsonb NOT NULL
);

CREATE INDEX capture_history_server_database_collected_at_idx
    ON fspg_ec_advisor.capture_history (server_id, database_oid, collected_at DESC);

CREATE TABLE fspg_ec_advisor.recommendation_feedback (
    feedback_id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    capture_key                    text NOT NULL
        REFERENCES fspg_ec_advisor.capture_history (capture_key)
        ON DELETE CASCADE,
    recorded_at                    timestamptz NOT NULL DEFAULT now(),
    operator_action                text NOT NULL,
    outcome                        text NOT NULL,
    notes                          text
);

CREATE TABLE fspg_ec_advisor.advisory_policy (
    policy_name                    text PRIMARY KEY,
    enabled                        boolean NOT NULL DEFAULT true,
    minimum_confidence             text NOT NULL DEFAULT 'MEDIUM'
        CHECK (minimum_confidence IN ('LOW', 'MEDIUM', 'HIGH')),
    pressure_window                interval NOT NULL DEFAULT interval '60 minutes'
        CHECK (pressure_window > interval '0 seconds'),
    minimum_pressure_samples       integer NOT NULL DEFAULT 3
        CHECK (minimum_pressure_samples > 0),
    minimum_window_seconds         integer NOT NULL DEFAULT 300
        CHECK (minimum_window_seconds >= 0),
    connection_utilization_pct     numeric NOT NULL DEFAULT 85
        CHECK (connection_utilization_pct BETWEEN 0 AND 100),
    waiting_active_pct             numeric NOT NULL DEFAULT 20
        CHECK (waiting_active_pct BETWEEN 0 AND 100),
    temp_bytes_per_second          numeric NOT NULL DEFAULT 16777216
        CHECK (temp_bytes_per_second >= 0),
    deadlocks_per_hour             numeric NOT NULL DEFAULT 1
        CHECK (deadlocks_per_hour >= 0),
    client_io_reads_per_second     numeric NOT NULL DEFAULT 1000
        CHECK (client_io_reads_per_second >= 0),
    client_io_writes_per_second    numeric NOT NULL DEFAULT 1000
        CHECK (client_io_writes_per_second >= 0),
    checkpoint_requests_per_hour   numeric NOT NULL DEFAULT 6
        CHECK (checkpoint_requests_per_hour >= 0),
    cpu_pct                        numeric NOT NULL DEFAULT 85
        CHECK (cpu_pct BETWEEN 0 AND 100),
    memory_pct                     numeric NOT NULL DEFAULT 85
        CHECK (memory_pct BETWEEN 0 AND 100),
    storage_latency_ms             numeric NOT NULL DEFAULT 20
        CHECK (storage_latency_ms >= 0),
    storage_queue_depth            numeric NOT NULL DEFAULT 5
        CHECK (storage_queue_depth >= 0),
    iops_utilization_pct           numeric NOT NULL DEFAULT 85
        CHECK (iops_utilization_pct BETWEEN 0 AND 100),
    application_latency_ms         numeric
        CHECK (application_latency_ms IS NULL OR application_latency_ms >= 0),
    slo_error_pct                  numeric NOT NULL DEFAULT 1
        CHECK (slo_error_pct BETWEEN 0 AND 100),
    telemetry_max_age              interval NOT NULL DEFAULT interval '15 minutes'
        CHECK (telemetry_max_age > interval '0 seconds'),
    notification_cooldown          interval NOT NULL DEFAULT interval '30 minutes'
        CHECK (notification_cooldown >= interval '0 seconds'),
    auto_resolve_after             interval NOT NULL DEFAULT interval '2 hours'
        CHECK (auto_resolve_after > interval '0 seconds'),
    azure_monitor_enabled          boolean NOT NULL DEFAULT true,
    created_at                     timestamptz NOT NULL DEFAULT now(),
    updated_at                     timestamptz NOT NULL DEFAULT now()
);

INSERT INTO fspg_ec_advisor.advisory_policy (policy_name)
VALUES ('default');

CREATE TABLE fspg_ec_advisor.infrastructure_telemetry (
    telemetry_id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_id                      text NOT NULL,
    database_oid                   integer NOT NULL,
    collected_at                   timestamptz NOT NULL,
    source                         text NOT NULL,
    cpu_pct                        numeric,
    memory_pct                     numeric,
    storage_latency_ms             numeric,
    storage_queue_depth            numeric,
    iops_utilization_pct           numeric,
    application_latency_ms         numeric,
    slo_error_pct                  numeric,
    payload                        jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at                    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX infrastructure_telemetry_server_database_collected_at_idx
    ON fspg_ec_advisor.infrastructure_telemetry (
        server_id,
        database_oid,
        collected_at DESC
    );

CREATE TABLE fspg_ec_advisor.advisory_event (
    event_id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_name                    text NOT NULL
        REFERENCES fspg_ec_advisor.advisory_policy (policy_name),
    capture_key                    text NOT NULL
        REFERENCES fspg_ec_advisor.capture_history (capture_key)
        ON DELETE CASCADE,
    server_id                      text NOT NULL,
    database_oid                   integer NOT NULL,
    dedupe_key                     text NOT NULL,
    action                         text NOT NULL,
    severity                       text NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    state                          text NOT NULL DEFAULT 'open'
        CHECK (state IN ('open', 'acknowledged', 'resolved', 'suppressed')),
    first_seen_at                  timestamptz NOT NULL DEFAULT now(),
    last_seen_at                   timestamptz NOT NULL DEFAULT now(),
    state_changed_at               timestamptz NOT NULL DEFAULT now(),
    acknowledged_at                timestamptz,
    suppressed_until               timestamptz,
    resolved_at                    timestamptz,
    occurrence_count               integer NOT NULL DEFAULT 1,
    last_notified_at               timestamptz,
    payload                        jsonb NOT NULL
);

CREATE UNIQUE INDEX advisory_event_open_dedupe_key_idx
    ON fspg_ec_advisor.advisory_event (dedupe_key)
    WHERE state IN ('open', 'acknowledged', 'suppressed');

CREATE INDEX advisory_event_server_database_state_idx
    ON fspg_ec_advisor.advisory_event (server_id, database_oid, state, last_seen_at DESC);

CREATE TABLE fspg_ec_advisor.azure_monitor_outbox (
    outbox_id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id                       bigint NOT NULL
        REFERENCES fspg_ec_advisor.advisory_event (event_id)
        ON DELETE CASCADE,
    event_type                     text NOT NULL
        CHECK (event_type IN ('activated', 'updated', 'resolved')),
    payload                        jsonb NOT NULL,
    status                         text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'claimed', 'delivered', 'failed')),
    attempts                       integer NOT NULL DEFAULT 0,
    available_at                   timestamptz NOT NULL DEFAULT now(),
    claimed_at                     timestamptz,
    lease_expires_at               timestamptz,
    delivered_at                   timestamptz,
    last_error                     text,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX azure_monitor_outbox_delivery_idx
    ON fspg_ec_advisor.azure_monitor_outbox (status, available_at, outbox_id);

CREATE FUNCTION fspg_ec_advisor._server_id()
RETURNS text
LANGUAGE plpgsql
STABLE
AS $plpgsql$
DECLARE
    v_server_id text;
BEGIN
    BEGIN
        SELECT system_identifier::text
        INTO v_server_id
        FROM pg_control_system();
    EXCEPTION WHEN insufficient_privilege THEN
        v_server_id := coalesce(inet_server_addr()::text, 'local')
                       || '_' || coalesce(inet_server_port()::text, '0');
    END;

    RETURN coalesce(
        nullif(left(regexp_replace(lower(v_server_id), '[^a-z0-9]+', '_', 'g'), 40), ''),
        'server'
    );
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor._wait_metrics()
RETURNS TABLE (
    queryid bigint,
    io_wait_fraction numeric,
    lock_wait_fraction numeric,
    ipc_wait_fraction numeric
)
LANGUAGE plpgsql
STABLE
AS $plpgsql$
BEGIN
    IF to_regclass('query_store.pgms_wait_sampling_view') IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY EXECUTE $sql$
        WITH sampled_waits AS (
            SELECT
                query_id::bigint AS queryid,
                event_type::text AS event_type,
                calls::numeric AS samples
            FROM query_store.pgms_wait_sampling_view
            WHERE db_id = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND query_id IS NOT NULL
        ),
        aggregates AS (
            SELECT
                queryid,
                sum(samples) AS wait_samples,
                sum(samples) FILTER (WHERE event_type = 'IO') AS io_wait_samples,
                sum(samples) FILTER (WHERE event_type IN ('Lock', 'LWLock', 'BufferPin'))
                    AS lock_wait_samples,
                sum(samples) FILTER (WHERE event_type = 'IPC') AS ipc_wait_samples
            FROM sampled_waits
            GROUP BY queryid
        )
        SELECT
            queryid,
            coalesce(io_wait_samples, 0) / nullif(wait_samples, 0),
            coalesce(lock_wait_samples, 0) / nullif(wait_samples, 0),
            coalesce(ipc_wait_samples, 0) / nullif(wait_samples, 0)
        FROM aggregates
    $sql$;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor._cross_node_metrics()
RETURNS TABLE (
    queryid bigint,
    is_cross_node integer
)
LANGUAGE plpgsql
STABLE
AS $plpgsql$
BEGIN
    IF to_regclass('pg_catalog.citus_stat_statements') IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY EXECUTE $sql$
        SELECT
            queryid,
            max(
                CASE
                    WHEN executor = 'router' AND partition_key IS NOT NULL THEN 0
                    ELSE 1
                END
            ) AS is_cross_node
        FROM pg_catalog.citus_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND queryid IS NOT NULL
        GROUP BY queryid
    $sql$;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.cluster_fairness()
RETURNS numeric
LANGUAGE plpgsql
STABLE
AS $plpgsql$
DECLARE
    v_fairness numeric;
BEGIN
    IF to_regclass('pg_catalog.citus_shards') IS NULL
       OR to_regclass('pg_catalog.pg_dist_node') IS NULL THEN
        RETURN NULL;
    END IF;

    EXECUTE $sql$
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
        per_node AS (
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
            CASE
                WHEN count(*) = 0 THEN NULL
                WHEN sum(node_bytes) = 0 THEN 1.00
                ELSE round(
                    greatest(0, 1 - stddev_pop(node_bytes) / nullif(avg(node_bytes), 0)),
                    2
                )
            END
        FROM per_node
    $sql$
    INTO v_fairness;

    RETURN v_fairness;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.query_scores()
RETURNS TABLE (
    queryid bigint,
    calls numeric,
    total_exec_ms numeric,
    mean_exec_ms numeric,
    rows_per_call numeric,
    io_weight numeric,
    oltp_raw numeric,
    olap_raw numeric,
    htap_raw numeric,
    timeseries_raw numeric,
    is_cross_node integer,
    query_sample text
)
LANGUAGE sql
VOLATILE
AS $sql$
    WITH settings AS (
        SELECT coalesce(current_setting('pg_stat_statements.track_planning', true)::boolean, false)
            AS planning_tracked
    ),
    base AS (
        SELECT
            stats.queryid,
            stats.calls::numeric AS calls,
            stats.total_exec_time::numeric AS total_exec_ms,
            coalesce(
                stats.mean_exec_time::numeric,
                stats.total_exec_time::numeric / nullif(stats.calls, 0),
                0
            ) AS mean_exec_ms,
            coalesce(stats.rows::numeric / nullif(stats.calls, 0), 0) AS rows_per_call,
            coalesce((stats.shared_blks_hit + stats.shared_blks_read)::numeric, 0)
                AS shared_blks_total,
            coalesce((stats.temp_blks_read + stats.temp_blks_written)::numeric, 0)
                AS temp_blks_total,
            coalesce((stats.local_blks_hit + stats.local_blks_read
                    + stats.local_blks_dirtied + stats.local_blks_written)::numeric, 0)
                AS local_blks_total,
            CASE WHEN settings.planning_tracked THEN stats.total_plan_time::numeric END
                AS total_plan_ms,
            stats.query,
            coalesce(wait_metrics.io_wait_fraction, 0) AS io_wait_fraction,
            coalesce(wait_metrics.lock_wait_fraction, 0) AS lock_wait_fraction,
            coalesce(wait_metrics.ipc_wait_fraction, 0) AS ipc_wait_fraction,
            coalesce(cross_node.is_cross_node, 0) AS is_cross_node
        FROM @extschema:pg_stat_statements@.pg_stat_statements AS stats
        CROSS JOIN settings
        LEFT JOIN fspg_ec_advisor._wait_metrics() AS wait_metrics
               ON wait_metrics.queryid = stats.queryid
        LEFT JOIN fspg_ec_advisor._cross_node_metrics() AS cross_node
               ON cross_node.queryid = stats.queryid
        WHERE stats.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND stats.toplevel
          AND stats.query !~* '\mfspg_ec_advisor\M|pgms_wait_sampling_view|citus_stat_statements'
          AND stats.query !~* '^\s*(begin|start|commit|end|rollback|savepoint|release|prepare|deallocate|set|reset|show|discard|lock|listen|unlisten|notify|create|drop|alter|truncate|vacuum|analyze|analyse|reindex|cluster|refresh|grant|revoke|comment|security|do|call|explain|copy)\M'
    ),
    features AS (
        SELECT
            *,
            total_plan_ms / nullif(total_plan_ms + total_exec_ms, 0) AS plan_fraction,
            temp_blks_total / nullif(shared_blks_total + temp_blks_total + local_blks_total, 0)
                AS temp_io_fraction,
            shared_blks_total / nullif(calls, 0) AS shared_blks_per_call,
            CASE WHEN query ~* '^\s*select\m' THEN 1 ELSE 0 END AS is_select,
            CASE WHEN query ~* '^\s*insert\m' THEN 1 ELSE 0 END AS is_insert,
            CASE WHEN query ~* '^\s*update\m' THEN 1 ELSE 0 END AS is_update,
            CASE WHEN query ~* '^\s*delete\m' THEN 1 ELSE 0 END AS is_delete,
            CASE WHEN query ~* '\mjoin\M' THEN 1 ELSE 0 END AS has_join,
            CASE WHEN query ~* '\mgroup\s+by\M' THEN 1 ELSE 0 END AS has_group_by,
            CASE WHEN query ~* '\mdistinct\M' THEN 1 ELSE 0 END AS has_distinct,
            CASE WHEN query ~* '\mhaving\M' THEN 1 ELSE 0 END AS has_having,
            CASE WHEN query ~* '\mover\s*\(' THEN 1 ELSE 0 END AS has_window,
            CASE WHEN query ~* '\mlimit\M' THEN 1 ELSE 0 END AS has_limit,
            CASE WHEN query ~* '\m(date_trunc|time_bucket|extract\s*\(|now\s*\(|current_timestamp|current_date)\M'
                THEN 1 ELSE 0 END AS has_time_function,
            CASE WHEN query ~* '\m(created_at|updated_at|event_time|event_ts|timestamp|ts|time)\M'
                THEN 1 ELSE 0 END AS has_time_column_hint,
            CASE WHEN query ~* '\minterval\M' THEN 1 ELSE 0 END AS has_interval,
            CASE WHEN query ~* '\morder\s+by\s+[^;]*\m(desc)\M'
                       AND query ~* '\mlimit\M'
                THEN 1 ELSE 0 END AS recent_window_pattern
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
            )::numeric AS oltp_raw,
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
            )::numeric AS olap_raw,
            (
                2.0 * CASE WHEN is_insert = 1 THEN 1 ELSE 0 END
              + 1.4 * CASE WHEN has_time_function = 1 OR has_time_column_hint = 1 THEN 1 ELSE 0 END
              + 1.0 * CASE WHEN has_interval = 1 THEN 1 ELSE 0 END
              + 1.0 * CASE WHEN recent_window_pattern = 1 THEN 1 ELSE 0 END
              + 0.8 * CASE WHEN rows_per_call BETWEEN 1 AND 100000 THEN 1 ELSE 0 END
              + 0.6 * CASE WHEN has_group_by = 1 AND (has_time_function = 1 OR has_time_column_hint = 1) THEN 1 ELSE 0 END
            )::numeric AS timeseries_raw,
            (
                2.0 * least(
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
            )::numeric AS htap_raw
        FROM features
    )
    SELECT
        queryid,
        calls,
        total_exec_ms,
        mean_exec_ms,
        rows_per_call,
        shared_blks_total + temp_blks_total + local_blks_total AS io_weight,
        oltp_raw,
        olap_raw,
        htap_raw,
        timeseries_raw,
        is_cross_node,
        left(regexp_replace(query, E'\\s+', ' ', 'g'), 220) AS query_sample
    FROM scored
$sql$;

CREATE FUNCTION fspg_ec_advisor.resource_snapshot()
RETURNS jsonb
LANGUAGE sql
VOLATILE
AS $sql$
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
    sessions AS (
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
    client_io AS (
        SELECT
            coalesce(sum(reads), 0)::numeric AS client_io_reads,
            coalesce(sum(writes), 0)::numeric AS client_io_writes,
            max(stats_reset) AS client_io_stats_reset
        FROM pg_stat_io
        WHERE backend_type = 'client backend'
    ),
    checkpoints AS (
        SELECT
            num_timed::numeric AS checkpoints_timed,
            num_requested::numeric AS checkpoints_requested,
            stats_reset AS checkpointer_stats_reset
        FROM pg_stat_checkpointer
    ),
    snapshot AS (
        SELECT
            connection_limits.max_connections,
            greatest(
                0,
                connection_limits.max_connections
                - connection_limits.reserved_connections
                - connection_limits.superuser_reserved_connections
            ) AS connection_capacity,
            connection_limits.track_io_timing,
            sessions.*,
            database_snapshot.*,
            client_io.*,
            checkpoints.*
        FROM connection_limits
        CROSS JOIN sessions
        CROSS JOIN database_snapshot
        CROSS JOIN client_io
        CROSS JOIN checkpoints
    )
    SELECT jsonb_build_object(
        'resource_pressure_status',
        CASE
            WHEN connection_capacity <= 0 THEN 'UNKNOWN_CONNECTION_CAPACITY'
            WHEN client_backends >= connection_capacity THEN 'SATURATED_CONNECTIONS'
            WHEN 100 * client_backends / nullif(connection_capacity, 0) >= 85
             AND waiting_client_backends > 0 THEN 'HIGH_CONNECTION_PRESSURE'
            WHEN 100 * client_backends / nullif(connection_capacity, 0) >= 85
                THEN 'HIGH_CONNECTION_UTILIZATION'
            ELSE 'NO_CONNECTION_PRESSURE_SIGNAL'
        END,
        'max_connections', max_connections,
        'connection_capacity', connection_capacity,
        'client_backends', client_backends,
        'active_client_backends', active_client_backends,
        'waiting_client_backends', waiting_client_backends,
        'idle_in_transaction_backends', idle_in_transaction_backends,
        'connection_utilization_pct', round(100 * client_backends / nullif(connection_capacity, 0), 2),
        'waiting_active_pct', round(100 * waiting_client_backends / nullif(active_client_backends, 0), 2),
        'track_io_timing', track_io_timing,
        'database_cache_hit_pct', round(100 * database_blks_hit / nullif(database_blks_hit + database_blks_read, 0), 2),
        'database_temp_bytes', database_temp_bytes,
        'database_deadlocks', database_deadlocks,
        'database_blk_read_time_ms', database_blk_read_time_ms,
        'database_blk_write_time_ms', database_blk_write_time_ms,
        'database_stats_reset', database_stats_reset,
        'client_io_reads', client_io_reads,
        'client_io_writes', client_io_writes,
        'client_io_stats_reset', client_io_stats_reset,
        'checkpoints_timed', checkpoints_timed,
        'checkpoints_requested', checkpoints_requested,
        'checkpointer_stats_reset', checkpointer_stats_reset
    )
    FROM snapshot
$sql$;

CREATE FUNCTION fspg_ec_advisor.capture(
    p_pressure_samples integer DEFAULT 3,
    p_pressure_window_minutes integer DEFAULT 60,
    p_min_window_seconds integer DEFAULT 300
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_server_id text := fspg_ec_advisor._server_id();
    v_database_oid integer;
    v_database_name text := current_database();
    v_capture_key text;
    v_pgss_stats_reset timestamptz;
    v_resource jsonb;
    v_fairness numeric;
    v_is_citus boolean;
    v_calls_total numeric;
    v_exec_ms_total numeric;
    v_io_blocks_total numeric;
    v_oltp_pct numeric;
    v_olap_pct numeric;
    v_htap_pct numeric;
    v_timeseries_pct numeric;
    v_ideal_local_pct numeric;
    v_cross_node_pct numeric;
    v_routability numeric;
    v_previous fspg_ec_advisor.capture_history%ROWTYPE;
    v_window_seconds numeric;
    v_window_blocker text;
    v_window_valid boolean := false;
    v_calls_delta numeric;
    v_exec_ms_delta numeric;
    v_temp_bytes_delta numeric;
    v_deadlocks_delta numeric;
    v_io_reads_delta numeric;
    v_io_writes_delta numeric;
    v_checkpoints_delta numeric;
    v_capture_count integer;
    v_pressured_count integer;
    v_sustained_pressure boolean := false;
    v_coverage numeric := 30;
    v_confidence text;
    v_action text;
    v_workload_fit text;
    v_scale_recommendation text;
    v_expected_benefit text;
    v_verification_step text;
    v_blocking_signals text;
    v_evidence_summary text;
    v_payload jsonb;
BEGIN
    IF p_pressure_samples < 1
       OR p_pressure_window_minutes < 1
       OR p_min_window_seconds < 0 THEN
        RAISE EXCEPTION 'pressure_samples and pressure_window_minutes must be positive; min_window_seconds cannot be negative';
    END IF;

    SELECT oid::integer
    INTO v_database_oid
    FROM pg_database
    WHERE datname = v_database_name;

    v_capture_key := md5(format('%s:%s:%s:%s', v_server_id, v_database_oid, pg_backend_pid(), v_now));

    SELECT stats_reset
    INTO v_pgss_stats_reset
    FROM @extschema:pg_stat_statements@.pg_stat_statements_info;

    SELECT
        coalesce(sum(calls), 0),
        coalesce(sum(total_exec_ms), 0),
        coalesce(sum(io_weight), 0),
        round(
            100 * sum(oltp_raw * total_exec_ms)
            / nullif(sum((oltp_raw + olap_raw + htap_raw + timeseries_raw) * total_exec_ms), 0),
            2
        ),
        round(
            100 * sum(olap_raw * total_exec_ms)
            / nullif(sum((oltp_raw + olap_raw + htap_raw + timeseries_raw) * total_exec_ms), 0),
            2
        ),
        round(
            100 * sum(htap_raw * total_exec_ms)
            / nullif(sum((oltp_raw + olap_raw + htap_raw + timeseries_raw) * total_exec_ms), 0),
            2
        ),
        round(
            100 * sum(timeseries_raw * total_exec_ms)
            / nullif(sum((oltp_raw + olap_raw + htap_raw + timeseries_raw) * total_exec_ms), 0),
            2
        ),
        round(
            100 * coalesce(sum(total_exec_ms) FILTER (WHERE is_cross_node = 0), 0)
            / nullif(sum(total_exec_ms), 0),
            2
        ),
        round(
            100 * coalesce(sum(total_exec_ms) FILTER (WHERE is_cross_node = 1), 0)
            / nullif(sum(total_exec_ms), 0),
            2
        ),
        round(
            sum((CASE WHEN is_cross_node = 1 THEN 0.5 ELSE 1.0 END) * total_exec_ms)
            / nullif(sum(total_exec_ms), 0),
            2
        )
    INTO
        v_calls_total,
        v_exec_ms_total,
        v_io_blocks_total,
        v_oltp_pct,
        v_olap_pct,
        v_htap_pct,
        v_timeseries_pct,
        v_ideal_local_pct,
        v_cross_node_pct,
        v_routability
    FROM fspg_ec_advisor.query_scores();

    v_oltp_pct := coalesce(v_oltp_pct, 0);
    v_olap_pct := coalesce(v_olap_pct, 0);
    v_htap_pct := coalesce(v_htap_pct, 0);
    v_timeseries_pct := coalesce(v_timeseries_pct, 0);
    v_resource := fspg_ec_advisor.resource_snapshot();
    v_fairness := fspg_ec_advisor.cluster_fairness();
    v_is_citus := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus');

    SELECT *
    INTO v_previous
    FROM fspg_ec_advisor.capture_history
    WHERE server_id = v_server_id
      AND database_oid = v_database_oid
    ORDER BY collected_at DESC
    LIMIT 1;

    IF FOUND THEN
        v_window_seconds := extract(epoch FROM v_now - v_previous.collected_at);

        IF v_pgss_stats_reset IS DISTINCT FROM v_previous.pgss_stats_reset
           OR (v_resource ->> 'database_stats_reset')::timestamptz
                IS DISTINCT FROM v_previous.database_stats_reset
           OR (v_resource ->> 'client_io_stats_reset')::timestamptz
                IS DISTINCT FROM v_previous.client_io_stats_reset
           OR (v_resource ->> 'checkpointer_stats_reset')::timestamptz
                IS DISTINCT FROM v_previous.checkpointer_stats_reset THEN
            v_window_blocker := 'STATISTICS_RESET';
        ELSIF v_window_seconds < p_min_window_seconds THEN
            v_window_blocker := 'WINDOW_TOO_SHORT';
        ELSIF v_calls_total < v_previous.workload_calls_total
           OR v_exec_ms_total < v_previous.workload_exec_ms_total
           OR v_io_blocks_total < v_previous.workload_io_blocks_total
           OR (v_resource ->> 'database_temp_bytes')::numeric < v_previous.database_temp_bytes
           OR (v_resource ->> 'database_deadlocks')::numeric < v_previous.database_deadlocks
           OR (v_resource ->> 'client_io_reads')::numeric < v_previous.client_io_reads
           OR (v_resource ->> 'client_io_writes')::numeric < v_previous.client_io_writes
           OR (v_resource ->> 'checkpoints_timed')::numeric < v_previous.checkpoints_timed
           OR (v_resource ->> 'checkpoints_requested')::numeric < v_previous.checkpoints_requested THEN
            v_window_blocker := 'COUNTER_REGRESSION';
        ELSE
            v_window_valid := true;
            v_calls_delta := v_calls_total - v_previous.workload_calls_total;
            v_exec_ms_delta := v_exec_ms_total - v_previous.workload_exec_ms_total;
            v_temp_bytes_delta := (v_resource ->> 'database_temp_bytes')::numeric
                - v_previous.database_temp_bytes;
            v_deadlocks_delta := (v_resource ->> 'database_deadlocks')::numeric
                - v_previous.database_deadlocks;
            v_io_reads_delta := (v_resource ->> 'client_io_reads')::numeric
                - v_previous.client_io_reads;
            v_io_writes_delta := (v_resource ->> 'client_io_writes')::numeric
                - v_previous.client_io_writes;
            v_checkpoints_delta := (v_resource ->> 'checkpoints_requested')::numeric
                - v_previous.checkpoints_requested;
        END IF;
    ELSE
        v_window_blocker := 'NO_PRIOR_CAPTURE';
    END IF;

    SELECT
        count(*),
        count(*) FILTER (
            WHERE resource_pressure_status IN (
                'SATURATED_CONNECTIONS',
                'HIGH_CONNECTION_PRESSURE',
                'HIGH_CONNECTION_UTILIZATION'
            )
        )
    INTO v_capture_count, v_pressured_count
    FROM (
        SELECT resource_pressure_status
        FROM (
            SELECT resource_pressure_status
            FROM fspg_ec_advisor.capture_history
            WHERE server_id = v_server_id
              AND database_oid = v_database_oid
              AND pgss_stats_reset IS NOT DISTINCT FROM v_pgss_stats_reset
              AND database_stats_reset IS NOT DISTINCT FROM (v_resource ->> 'database_stats_reset')::timestamptz
              AND client_io_stats_reset IS NOT DISTINCT FROM (v_resource ->> 'client_io_stats_reset')::timestamptz
              AND checkpointer_stats_reset IS NOT DISTINCT FROM (v_resource ->> 'checkpointer_stats_reset')::timestamptz
              AND collected_at >= v_now - make_interval(mins => p_pressure_window_minutes)
            ORDER BY collected_at DESC
            LIMIT greatest(p_pressure_samples - 1, 0)
        ) AS recent

        UNION ALL

        SELECT v_resource ->> 'resource_pressure_status'
    ) AS observations;

    v_sustained_pressure := v_window_valid
        AND v_capture_count >= p_pressure_samples
        AND v_pressured_count >= p_pressure_samples;

    IF v_window_valid THEN
        v_coverage := v_coverage + 30;
    END IF;
    IF coalesce((v_resource ->> 'track_io_timing')::boolean, false) THEN
        v_coverage := v_coverage + 10;
    END IF;
    IF to_regclass('query_store.pgms_wait_sampling_view') IS NOT NULL THEN
        v_coverage := v_coverage + 10;
    END IF;
    IF NOT v_is_citus
       OR (v_fairness IS NOT NULL AND to_regclass('pg_catalog.citus_stat_statements') IS NOT NULL) THEN
        v_coverage := v_coverage + 20;
    END IF;

    IF v_window_valid AND v_capture_count >= p_pressure_samples AND v_coverage >= 80 THEN
        v_confidence := 'HIGH';
    ELSIF v_window_valid AND v_coverage >= 60 THEN
        v_confidence := 'MEDIUM';
    ELSE
        v_confidence := 'LOW';
    END IF;

    IF v_oltp_pct + v_olap_pct + v_htap_pct + v_timeseries_pct = 0 THEN
        v_workload_fit := 'NO_DATA';
    ELSIF v_fairness IS NOT NULL AND v_fairness < 0.85 THEN
        v_workload_fit := 'REBALANCE_FIRST';
    ELSIF coalesce(v_cross_node_pct, 0) >= 40 OR coalesce(v_routability, 1) < 0.60 THEN
        v_workload_fit := 'OPTIMIZE_LOCALITY_FIRST';
    ELSIF v_olap_pct + v_htap_pct >= 40 AND v_is_citus THEN
        v_workload_fit := 'SCALE_OUT';
    ELSIF v_olap_pct + v_htap_pct >= 40 THEN
        v_workload_fit := 'CITUS_MIGRATION_CANDIDATE';
    ELSIF v_oltp_pct >= 50 AND v_is_citus THEN
        v_workload_fit := 'SCALE_OUT';
    ELSIF v_oltp_pct >= 50 THEN
        v_workload_fit := 'CITUS_MIGRATION_CANDIDATE';
    ELSE
        v_workload_fit := 'REVIEW';
    END IF;

    IF v_workload_fit = 'NO_DATA' OR v_confidence = 'LOW' THEN
        v_action := 'REVIEW_TELEMETRY';
    ELSIF v_workload_fit = 'REBALANCE_FIRST' THEN
        v_action := 'REBALANCE_FIRST';
    ELSIF v_workload_fit = 'OPTIMIZE_LOCALITY_FIRST' THEN
        v_action := 'OPTIMIZE_LOCALITY_FIRST';
    ELSIF NOT v_sustained_pressure THEN
        v_action := 'MONITOR';
    ELSIF (v_resource ->> 'resource_pressure_status') IN (
        'SATURATED_CONNECTIONS',
        'HIGH_CONNECTION_PRESSURE',
        'HIGH_CONNECTION_UTILIZATION'
    ) AND v_oltp_pct >= 50 THEN
        v_action := 'ADD_CONNECTION_POOLING';
    ELSIF v_olap_pct + v_htap_pct >= 40 AND v_is_citus THEN
        v_action := 'ADD_CITUS_WORKERS_CANDIDATE';
    ELSIF v_olap_pct + v_htap_pct >= 40 THEN
        v_action := 'CITUS_MIGRATION_CANDIDATE';
    ELSIF v_oltp_pct >= 50 AND v_is_citus THEN
        v_action := 'ADD_CITUS_WORKERS_CANDIDATE';
    ELSIF v_oltp_pct >= 50 THEN
        v_action := 'CITUS_MIGRATION_CANDIDATE';
    ELSIF v_sustained_pressure THEN
        v_action := 'SCALE_UP_CANDIDATE';
    ELSE
        v_action := 'TUNE_QUERY_OR_INDEX';
    END IF;

    v_blocking_signals := concat_ws(
        '; ',
        CASE WHEN v_window_blocker IS NOT NULL THEN lower(replace(v_window_blocker, '_', ' ')) END,
        CASE WHEN v_capture_count < p_pressure_samples
            THEN format('need %s captures in %s minutes', p_pressure_samples, p_pressure_window_minutes)
        END,
        CASE WHEN NOT v_sustained_pressure THEN 'resource pressure not sustained' END,
        CASE WHEN NOT coalesce((v_resource ->> 'track_io_timing')::boolean, false)
            THEN 'track_io_timing is off'
        END,
        CASE WHEN to_regclass('query_store.pgms_wait_sampling_view') IS NULL
            THEN 'wait sampling unavailable'
        END,
        CASE WHEN v_is_citus AND v_fairness IS NULL THEN 'Citus fairness unavailable' END
    );

    v_evidence_summary := concat_ws(
        '; ',
        format('window=%s seconds', coalesce(round(v_window_seconds, 1)::text, 'n/a')),
        format('pressure captures=%s/%s', v_pressured_count, p_pressure_samples),
        format('connection=%s%%', coalesce(v_resource ->> 'connection_utilization_pct', 'n/a')),
        format('waiting active=%s%%', coalesce(v_resource ->> 'waiting_active_pct', 'n/a')),
        format('workload exec=%s ms/s', coalesce(round(v_exec_ms_delta / nullif(v_window_seconds, 0), 2)::text, 'n/a'))
    );

    SELECT
        CASE v_action
            WHEN 'REBALANCE_FIRST' THEN 'REBALANCE_FIRST: shard-byte fairness is below the safe scale-out threshold.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST' THEN 'OPTIMIZE_LOCALITY_FIRST: cross-node execution or routability blocks efficient scaling.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE' THEN 'SCALE_OUT: sustained pressure and workload fit support adding Citus worker capacity.'
            WHEN 'CITUS_MIGRATION_CANDIDATE' THEN 'SCALE_OUT: sustained pressure and workload fit support evaluating Citus / Azure Elastic Clusters.'
            WHEN 'ADD_CONNECTION_POOLING' THEN 'SCALE_UP_OR_TUNE: sustained connection pressure favors pooling before database scaling.'
            WHEN 'SCALE_UP_CANDIDATE' THEN 'SCALE_UP_OR_TUNE: sustained pressure lacks a strong distributed-scale fit.'
            WHEN 'MONITOR' THEN 'REVIEW: workload fit exists, but resource pressure is not sustained.'
            WHEN 'TUNE_QUERY_OR_INDEX' THEN 'REVIEW: sustained pressure exists without a clear scale fit; inspect query evidence.'
            ELSE 'REVIEW: insufficient reset-safe telemetry for an operator action.'
        END,
        CASE v_action
            WHEN 'REBALANCE_FIRST' THEN 'Restore shard balance before adding workers.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST' THEN 'Reduce scatter/gather work before scaling.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE' THEN 'Increase distributed execution capacity.'
            WHEN 'CITUS_MIGRATION_CANDIDATE' THEN 'Evaluate horizontal scale after a Citus design review.'
            WHEN 'ADD_CONNECTION_POOLING' THEN 'Lower backend pressure without immediately adding database compute.'
            WHEN 'SCALE_UP_CANDIDATE' THEN 'Increase single-node compute or storage capacity.'
            WHEN 'MONITOR' THEN 'Avoid unnecessary scaling until sustained pressure is demonstrated.'
            WHEN 'TUNE_QUERY_OR_INDEX' THEN 'Reduce inefficient work before adding capacity.'
            ELSE 'Collect a reset-safe capture window with sufficient telemetry coverage.'
        END,
        CASE v_action
            WHEN 'REBALANCE_FIRST' THEN 'Re-run after rebalance and confirm fairness, locality, and pressure.'
            WHEN 'OPTIMIZE_LOCALITY_FIRST' THEN 'Re-run after distribution changes and confirm lower cross-node execution.'
            WHEN 'ADD_CITUS_WORKERS_CANDIDATE' THEN 'Compare pressure and routability across the next three captures.'
            WHEN 'CITUS_MIGRATION_CANDIDATE' THEN 'Complete the Phase 2 Citus readiness assessment before migration.'
            WHEN 'ADD_CONNECTION_POOLING' THEN 'Confirm reduced connection utilization and waiting-session share.'
            WHEN 'SCALE_UP_CANDIDATE' THEN 'Confirm lower resource pressure across three reset-safe captures.'
            WHEN 'MONITOR' THEN 'Collect the remaining captures inside the configured pressure window.'
            WHEN 'TUNE_QUERY_OR_INDEX' THEN 'Inspect high execution-time fingerprints and validate a tuning change.'
            ELSE 'Collect a valid baseline and enough capture intervals to establish confidence.'
        END
    INTO v_scale_recommendation, v_expected_benefit, v_verification_step;

    v_payload := jsonb_build_object(
        'capture_key', v_capture_key,
        'collected_at', v_now,
        'server_id', v_server_id,
        'database', jsonb_build_object('oid', v_database_oid, 'name', v_database_name),
        'workload', jsonb_build_object(
            'oltp_pct', v_oltp_pct,
            'olap_pct', v_olap_pct,
            'htap_pct', v_htap_pct,
            'timeseries_pct', v_timeseries_pct,
            'calls_total', v_calls_total,
            'execution_ms_total', v_exec_ms_total,
            'io_blocks_total', v_io_blocks_total,
            'ideal_local_exec_pct', v_ideal_local_pct,
            'cross_node_exec_pct', v_cross_node_pct,
            'mean_routability_score', v_routability,
            'data_distribution_fairness', v_fairness,
            'citus_installed', v_is_citus,
            'workload_fit_recommendation', v_workload_fit
        ),
        'resource', v_resource,
        'window', jsonb_build_object(
            'valid', v_window_valid,
            'blocker', v_window_blocker,
            'seconds', v_window_seconds,
            'calls_delta', v_calls_delta,
            'execution_ms_delta', v_exec_ms_delta,
            'temp_bytes_delta', v_temp_bytes_delta,
            'deadlocks_delta', v_deadlocks_delta,
            'client_io_reads_delta', v_io_reads_delta,
            'client_io_writes_delta', v_io_writes_delta,
            'checkpoint_requests_delta', v_checkpoints_delta,
            'temp_bytes_per_second', round(v_temp_bytes_delta / nullif(v_window_seconds, 0), 4),
            'deadlocks_per_hour', round(v_deadlocks_delta * 3600 / nullif(v_window_seconds, 0), 4),
            'client_io_reads_per_second', round(v_io_reads_delta / nullif(v_window_seconds, 0), 4),
            'client_io_writes_per_second', round(v_io_writes_delta / nullif(v_window_seconds, 0), 4),
            'checkpoint_requests_per_hour', round(v_checkpoints_delta * 3600 / nullif(v_window_seconds, 0), 4),
            'sustained_resource_pressure', v_sustained_pressure,
            'pressure_capture_count', v_pressured_count,
            'required_pressure_samples', p_pressure_samples
        ),
        'recommendation', jsonb_build_object(
            'action', v_action,
            'scale_recommendation', v_scale_recommendation,
            'confidence', v_confidence,
            'telemetry_coverage_pct', v_coverage,
            'blocking_signals', v_blocking_signals,
            'evidence_summary', v_evidence_summary,
            'expected_benefit', v_expected_benefit,
            'verification_step', v_verification_step
        )
    );

    INSERT INTO fspg_ec_advisor.capture_history (
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
        database_temp_bytes,
        database_deadlocks,
        client_io_reads,
        client_io_writes,
        checkpoints_timed,
        checkpoints_requested,
        resource_pressure_status,
        recommended_action,
        recommendation_confidence,
        telemetry_coverage_pct,
        payload
    )
    VALUES (
        v_capture_key,
        v_now,
        v_server_id,
        v_database_oid,
        v_database_name,
        v_pgss_stats_reset,
        (v_resource ->> 'database_stats_reset')::timestamptz,
        (v_resource ->> 'client_io_stats_reset')::timestamptz,
        (v_resource ->> 'checkpointer_stats_reset')::timestamptz,
        v_calls_total,
        v_exec_ms_total,
        v_io_blocks_total,
        (v_resource ->> 'database_temp_bytes')::numeric,
        (v_resource ->> 'database_deadlocks')::numeric,
        (v_resource ->> 'client_io_reads')::numeric,
        (v_resource ->> 'client_io_writes')::numeric,
        (v_resource ->> 'checkpoints_timed')::numeric,
        (v_resource ->> 'checkpoints_requested')::numeric,
        v_resource ->> 'resource_pressure_status',
        v_action,
        v_confidence,
        v_coverage,
        v_payload
    );

    RETURN v_payload;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.latest_advice()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $sql$
    SELECT payload
    FROM fspg_ec_advisor.capture_history
    WHERE server_id = fspg_ec_advisor._server_id()
      AND database_oid = (SELECT oid::integer FROM pg_database WHERE datname = current_database())
    ORDER BY collected_at DESC
    LIMIT 1
$sql$;

CREATE FUNCTION fspg_ec_advisor.record_feedback(
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
    INSERT INTO fspg_ec_advisor.recommendation_feedback (
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

CREATE VIEW fspg_ec_advisor.recommendation_calibration AS
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
        / nullif(count(feedback.feedback_id), 0),
        2
    ) AS positive_outcome_pct
FROM fspg_ec_advisor.capture_history AS history
LEFT JOIN fspg_ec_advisor.recommendation_feedback AS feedback
       ON feedback.capture_key = history.capture_key
GROUP BY
    history.recommended_action,
    history.recommendation_confidence;

-- SQL-only extensions cannot register a typed custom GUC. Treat an unset
-- fspg_ec_advisor.reset_stats setting as off, and require an explicit session
-- SET before resetting pg_stat_statements.
CREATE FUNCTION fspg_ec_advisor.reset_stats()
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_reset_enabled boolean;
BEGIN
    BEGIN
        v_reset_enabled := coalesce(
            current_setting('fspg_ec_advisor.reset_stats', true)::boolean,
            false
        );
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE INFO 'fspg_ec_advisor did not reset pg_stat_statements because fspg_ec_advisor.reset_stats is invalid'
            USING HINT = 'Set fspg_ec_advisor.reset_stats to on or off; invalid values are treated as off.';
        RETURN false;
    END;

    IF NOT v_reset_enabled THEN
        RAISE INFO 'fspg_ec_advisor did not reset pg_stat_statements because fspg_ec_advisor.reset_stats is off'
            USING HINT = 'Set fspg_ec_advisor.reset_stats = on before calling fspg_ec_advisor.reset_stats().';
        RETURN false;
    END IF;

    BEGIN
        PERFORM @extschema:pg_stat_statements@.pg_stat_statements_reset();
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE INFO 'fspg_ec_advisor could not reset pg_stat_statements because the current role lacks permission'
            USING HINT = 'Grant the privilege required by pg_stat_statements_reset() or leave fspg_ec_advisor.reset_stats off.';
        RETURN false;
    END;

    RETURN true;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.capture_and_reset(
    p_pressure_samples integer DEFAULT 3,
    p_pressure_window_minutes integer DEFAULT 60,
    p_min_window_seconds integer DEFAULT 300
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_capture jsonb;
BEGIN
    v_capture := fspg_ec_advisor.capture(
        p_pressure_samples,
        p_pressure_window_minutes,
        p_min_window_seconds
    );

    PERFORM fspg_ec_advisor.reset_stats();

    RETURN v_capture;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor._confidence_rank(p_confidence text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
STRICT
AS $sql$
    SELECT CASE upper(p_confidence)
        WHEN 'LOW' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'HIGH' THEN 3
        ELSE 0
    END
$sql$;

CREATE FUNCTION fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source text,
    p_collected_at timestamptz DEFAULT clock_timestamp(),
    p_cpu_pct numeric DEFAULT NULL,
    p_memory_pct numeric DEFAULT NULL,
    p_storage_latency_ms numeric DEFAULT NULL,
    p_storage_queue_depth numeric DEFAULT NULL,
    p_iops_utilization_pct numeric DEFAULT NULL,
    p_application_latency_ms numeric DEFAULT NULL,
    p_slo_error_pct numeric DEFAULT NULL,
    p_payload jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_telemetry_id bigint;
BEGIN
    IF nullif(btrim(p_source), '') IS NULL THEN
        RAISE EXCEPTION 'Telemetry source cannot be empty';
    END IF;

    IF p_collected_at > clock_timestamp() + interval '5 minutes' THEN
        RAISE EXCEPTION 'Telemetry collection timestamp cannot be more than five minutes in the future';
    END IF;

    IF (p_cpu_pct IS NOT NULL AND p_cpu_pct NOT BETWEEN 0 AND 100)
       OR (p_memory_pct IS NOT NULL AND p_memory_pct NOT BETWEEN 0 AND 100)
       OR (p_iops_utilization_pct IS NOT NULL AND p_iops_utilization_pct NOT BETWEEN 0 AND 100)
       OR (p_slo_error_pct IS NOT NULL AND p_slo_error_pct NOT BETWEEN 0 AND 100)
       OR (p_storage_latency_ms IS NOT NULL AND p_storage_latency_ms < 0)
       OR (p_storage_queue_depth IS NOT NULL AND p_storage_queue_depth < 0)
       OR (p_application_latency_ms IS NOT NULL AND p_application_latency_ms < 0) THEN
        RAISE EXCEPTION 'Infrastructure telemetry contains an invalid metric value';
    END IF;

    INSERT INTO fspg_ec_advisor.infrastructure_telemetry (
        server_id,
        database_oid,
        collected_at,
        source,
        cpu_pct,
        memory_pct,
        storage_latency_ms,
        storage_queue_depth,
        iops_utilization_pct,
        application_latency_ms,
        slo_error_pct,
        payload
    )
    VALUES (
        fspg_ec_advisor._server_id(),
        (SELECT oid::integer FROM pg_database WHERE datname = current_database()),
        p_collected_at,
        p_source,
        p_cpu_pct,
        p_memory_pct,
        p_storage_latency_ms,
        p_storage_queue_depth,
        p_iops_utilization_pct,
        p_application_latency_ms,
        p_slo_error_pct,
        coalesce(p_payload, '{}'::jsonb)
    )
    RETURNING telemetry_id INTO v_telemetry_id;

    RETURN v_telemetry_id;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.acknowledge_advisory_event(p_event_id bigint)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_updated integer;
BEGIN
    UPDATE fspg_ec_advisor.advisory_event
    SET
        state = 'acknowledged',
        acknowledged_at = clock_timestamp(),
        state_changed_at = clock_timestamp()
    WHERE event_id = p_event_id
      AND state IN ('open', 'acknowledged');

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.suppress_advisory_event(
    p_event_id bigint,
    p_suppress_seconds integer
)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_updated integer;
BEGIN
    IF p_suppress_seconds <= 0 THEN
        RAISE EXCEPTION 'Suppression duration must be positive';
    END IF;

    UPDATE fspg_ec_advisor.advisory_event
    SET
        state = 'suppressed',
        suppressed_until = clock_timestamp() + make_interval(secs => p_suppress_seconds),
        state_changed_at = clock_timestamp()
    WHERE event_id = p_event_id
      AND state IN ('open', 'acknowledged', 'suppressed');

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.claim_azure_monitor_outbox(
    p_limit integer DEFAULT 50,
    p_lease_seconds integer DEFAULT 300
)
RETURNS TABLE (
    outbox_id bigint,
    event_id bigint,
    event_type text,
    payload jsonb,
    attempts integer
)
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
BEGIN
    IF p_limit <= 0 OR p_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'Outbox limit and lease duration must be positive';
    END IF;

    RETURN QUERY
    WITH candidates AS (
        SELECT queue.outbox_id
        FROM fspg_ec_advisor.azure_monitor_outbox AS queue
        WHERE (queue.status = 'pending' AND queue.available_at <= clock_timestamp())
           OR (queue.status = 'claimed' AND queue.lease_expires_at <= clock_timestamp())
        ORDER BY queue.available_at, queue.outbox_id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    ),
    claimed AS (
        UPDATE fspg_ec_advisor.azure_monitor_outbox AS queue
        SET
            status = 'claimed',
            attempts = queue.attempts + 1,
            claimed_at = clock_timestamp(),
            lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
        FROM candidates
        WHERE queue.outbox_id = candidates.outbox_id
        RETURNING queue.outbox_id, queue.event_id, queue.event_type, queue.payload, queue.attempts
    )
    SELECT *
    FROM claimed;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.complete_azure_monitor_delivery(
    p_outbox_id bigint,
    p_success boolean,
    p_error text DEFAULT NULL,
    p_max_attempts integer DEFAULT 8
)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_updated integer;
BEGIN
    IF p_max_attempts <= 0 THEN
        RAISE EXCEPTION 'Maximum delivery attempts must be positive';
    END IF;

    IF p_success THEN
        UPDATE fspg_ec_advisor.azure_monitor_outbox
        SET
            status = 'delivered',
            delivered_at = clock_timestamp(),
            lease_expires_at = NULL,
            last_error = NULL
        WHERE outbox_id = p_outbox_id
          AND status = 'claimed';
    ELSE
        UPDATE fspg_ec_advisor.azure_monitor_outbox
        SET
            status = CASE WHEN attempts >= p_max_attempts THEN 'failed' ELSE 'pending' END,
            available_at = CASE
                WHEN attempts >= p_max_attempts THEN available_at
                ELSE clock_timestamp()
                    + make_interval(secs => least(3600, 30 * (2 ^ least(attempts, 6))::integer))
            END,
            lease_expires_at = NULL,
            last_error = coalesce(p_error, 'Azure Monitor delivery failed')
        WHERE outbox_id = p_outbox_id
          AND status = 'claimed';
    END IF;

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor._enqueue_azure_monitor_outbox(
    p_event_id bigint,
    p_event_type text,
    p_payload jsonb
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_outbox_id bigint;
BEGIN
    INSERT INTO fspg_ec_advisor.azure_monitor_outbox (
        event_id,
        event_type,
        payload
    )
    VALUES (
        p_event_id,
        p_event_type,
        p_payload
    )
    RETURNING outbox_id INTO v_outbox_id;

    -- NOTIFY is a latency hint only. The dispatcher must always poll the
    -- durable outbox because notifications are not retained for offline clients.
    PERFORM pg_notify('fspg_ec_advisor_azure_monitor', v_outbox_id::text);

    RETURN v_outbox_id;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.resolve_stale_advisories()
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_event record;
    v_resolved_count integer := 0;
    v_resolution_payload jsonb;
BEGIN
    FOR v_event IN
        WITH resolved AS (
            UPDATE fspg_ec_advisor.advisory_event AS event
            SET
                state = 'resolved',
                resolved_at = clock_timestamp(),
                state_changed_at = clock_timestamp()
            FROM fspg_ec_advisor.advisory_policy AS policy
            WHERE event.policy_name = policy.policy_name
              AND event.state IN ('open', 'acknowledged', 'suppressed')
              AND event.last_seen_at < clock_timestamp() - policy.auto_resolve_after
            RETURNING
                event.event_id,
                event.policy_name,
                event.action,
                event.severity,
                event.payload,
                event.server_id,
                event.database_oid
        )
        SELECT *
        FROM resolved
    LOOP
        v_resolution_payload := jsonb_build_object(
            'schemaVersion', '1.0',
            'eventType', 'fspg_ec_advisor.advisory.resolved',
            'eventTime', clock_timestamp(),
            'data', jsonb_build_object(
                'eventId', v_event.event_id,
                'policy', v_event.policy_name,
                'action', v_event.action,
                'severity', v_event.severity,
                'serverId', v_event.server_id,
                'databaseOid', v_event.database_oid,
                'reason', 'No matching advisory capture arrived before auto_resolve_after.',
                'priorEvent', v_event.payload
            )
        );

        IF (SELECT azure_monitor_enabled
            FROM fspg_ec_advisor.advisory_policy
            WHERE policy_name = v_event.policy_name) THEN
            PERFORM fspg_ec_advisor._enqueue_azure_monitor_outbox(
                v_event.event_id,
                'resolved',
                v_resolution_payload
            );
        END IF;

        v_resolved_count := v_resolved_count + 1;
    END LOOP;

    RETURN v_resolved_count;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.evaluate_capture(
    p_capture_key text,
    p_policy_name text DEFAULT 'default'
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_capture fspg_ec_advisor.capture_history%ROWTYPE;
    v_policy fspg_ec_advisor.advisory_policy%ROWTYPE;
    v_telemetry fspg_ec_advisor.infrastructure_telemetry%ROWTYPE;
    v_has_telemetry boolean := false;
    v_connection_pressure boolean := false;
    v_rate_pressure boolean := false;
    v_infrastructure_pressure boolean := false;
    v_infrastructure_sample_count integer := 0;
    v_infrastructure_pressure_count integer := 0;
    v_sustained_infrastructure_pressure boolean := false;
    v_confidence_ok boolean := false;
    v_sustained_pressure boolean := false;
    v_valid_capture_count integer := 0;
    v_pressure_capture_count integer := 0;
    v_action text;
    v_workload_fit text;
    v_severity text;
    v_dedupe_key text;
    v_event fspg_ec_advisor.advisory_event%ROWTYPE;
    v_existing_event boolean := false;
    v_event_type text;
    v_event_payload jsonb;
    v_outbox_id bigint;
    v_should_notify boolean := false;
    v_resolved record;
    v_resolved_count integer := 0;
BEGIN
    SELECT *
    INTO v_capture
    FROM fspg_ec_advisor.capture_history
    WHERE capture_key = p_capture_key;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown advisor capture key: %', p_capture_key;
    END IF;

    SELECT *
    INTO v_policy
    FROM fspg_ec_advisor.advisory_policy
    WHERE policy_name = p_policy_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown advisor policy: %', p_policy_name;
    END IF;

    IF NOT v_policy.enabled THEN
        RETURN jsonb_build_object(
            'state', 'disabled',
            'policy', v_policy.policy_name,
            'captureKey', v_capture.capture_key
        );
    END IF;

    SELECT *
    INTO v_telemetry
    FROM fspg_ec_advisor.infrastructure_telemetry
    WHERE server_id = v_capture.server_id
      AND database_oid = v_capture.database_oid
      AND collected_at >= v_capture.collected_at - v_policy.telemetry_max_age
    ORDER BY collected_at DESC
    LIMIT 1;
    v_has_telemetry := FOUND;

    v_connection_pressure :=
        v_capture.resource_pressure_status IN (
            'SATURATED_CONNECTIONS',
            'HIGH_CONNECTION_PRESSURE',
            'HIGH_CONNECTION_UTILIZATION'
        )
        OR (
            coalesce((v_capture.payload #>> '{resource,connection_utilization_pct}')::numeric, 0)
                >= v_policy.connection_utilization_pct
            AND coalesce((v_capture.payload #>> '{resource,waiting_active_pct}')::numeric, 0)
                >= v_policy.waiting_active_pct
        );

    v_rate_pressure :=
        coalesce((v_capture.payload #>> '{window,temp_bytes_per_second}')::numeric, 0)
            >= v_policy.temp_bytes_per_second
        OR coalesce((v_capture.payload #>> '{window,deadlocks_per_hour}')::numeric, 0)
            >= v_policy.deadlocks_per_hour
        OR coalesce((v_capture.payload #>> '{window,client_io_reads_per_second}')::numeric, 0)
            >= v_policy.client_io_reads_per_second
        OR coalesce((v_capture.payload #>> '{window,client_io_writes_per_second}')::numeric, 0)
            >= v_policy.client_io_writes_per_second
        OR coalesce((v_capture.payload #>> '{window,checkpoint_requests_per_hour}')::numeric, 0)
            >= v_policy.checkpoint_requests_per_hour;

    IF v_has_telemetry THEN
        v_infrastructure_pressure :=
            coalesce(v_telemetry.cpu_pct, 0) >= v_policy.cpu_pct
            OR coalesce(v_telemetry.memory_pct, 0) >= v_policy.memory_pct
            OR coalesce(v_telemetry.storage_latency_ms, 0) >= v_policy.storage_latency_ms
            OR coalesce(v_telemetry.storage_queue_depth, 0) >= v_policy.storage_queue_depth
            OR coalesce(v_telemetry.iops_utilization_pct, 0) >= v_policy.iops_utilization_pct
            OR (
                v_policy.application_latency_ms IS NOT NULL
                AND coalesce(v_telemetry.application_latency_ms, 0) >= v_policy.application_latency_ms
            )
            OR coalesce(v_telemetry.slo_error_pct, 0) >= v_policy.slo_error_pct;
    END IF;

    SELECT
        count(*),
        count(*) FILTER (
            WHERE coalesce(cpu_pct, 0) >= v_policy.cpu_pct
               OR coalesce(memory_pct, 0) >= v_policy.memory_pct
               OR coalesce(storage_latency_ms, 0) >= v_policy.storage_latency_ms
               OR coalesce(storage_queue_depth, 0) >= v_policy.storage_queue_depth
               OR coalesce(iops_utilization_pct, 0) >= v_policy.iops_utilization_pct
               OR (
                    v_policy.application_latency_ms IS NOT NULL
                    AND coalesce(application_latency_ms, 0) >= v_policy.application_latency_ms
               )
               OR coalesce(slo_error_pct, 0) >= v_policy.slo_error_pct
        )
    INTO v_infrastructure_sample_count, v_infrastructure_pressure_count
    FROM (
        SELECT
            cpu_pct,
            memory_pct,
            storage_latency_ms,
            storage_queue_depth,
            iops_utilization_pct,
            application_latency_ms,
            slo_error_pct
        FROM fspg_ec_advisor.infrastructure_telemetry
        WHERE server_id = v_capture.server_id
          AND database_oid = v_capture.database_oid
          AND collected_at >= v_capture.collected_at - v_policy.pressure_window
                    AND collected_at >= v_capture.collected_at - v_policy.telemetry_max_age
        ORDER BY collected_at DESC
        LIMIT v_policy.minimum_pressure_samples
    ) AS recent_telemetry;

    v_sustained_infrastructure_pressure :=
        v_infrastructure_sample_count >= v_policy.minimum_pressure_samples
        AND v_infrastructure_pressure_count >= v_policy.minimum_pressure_samples;

    SELECT
        count(*),
        count(*) FILTER (
            WHERE resource_pressure_status IN (
                'SATURATED_CONNECTIONS',
                'HIGH_CONNECTION_PRESSURE',
                'HIGH_CONNECTION_UTILIZATION'
            )
            OR coalesce((payload #>> '{window,temp_bytes_per_second}')::numeric, 0)
                >= v_policy.temp_bytes_per_second
            OR coalesce((payload #>> '{window,deadlocks_per_hour}')::numeric, 0)
                >= v_policy.deadlocks_per_hour
            OR coalesce((payload #>> '{window,client_io_reads_per_second}')::numeric, 0)
                >= v_policy.client_io_reads_per_second
            OR coalesce((payload #>> '{window,client_io_writes_per_second}')::numeric, 0)
                >= v_policy.client_io_writes_per_second
            OR coalesce((payload #>> '{window,checkpoint_requests_per_hour}')::numeric, 0)
                >= v_policy.checkpoint_requests_per_hour
        )
    INTO v_valid_capture_count, v_pressure_capture_count
    FROM (
        SELECT payload, resource_pressure_status
        FROM fspg_ec_advisor.capture_history
        WHERE server_id = v_capture.server_id
          AND database_oid = v_capture.database_oid
          AND collected_at >= v_capture.collected_at - v_policy.pressure_window
          AND coalesce((payload #>> '{window,valid}')::boolean, false)
        ORDER BY collected_at DESC
        LIMIT v_policy.minimum_pressure_samples
    ) AS recent;

    v_sustained_pressure :=
        coalesce((v_capture.payload #>> '{window,valid}')::boolean, false)
        AND v_valid_capture_count >= v_policy.minimum_pressure_samples
        AND (
            v_pressure_capture_count >= v_policy.minimum_pressure_samples
            OR v_sustained_infrastructure_pressure
        );

    v_confidence_ok :=
        fspg_ec_advisor._confidence_rank(v_capture.recommendation_confidence)
        >= fspg_ec_advisor._confidence_rank(v_policy.minimum_confidence);
    v_workload_fit := v_capture.payload #>> '{workload,workload_fit_recommendation}';

    IF NOT v_confidence_ok THEN
        v_action := NULL;
    ELSIF v_capture.recommended_action IN ('REBALANCE_FIRST', 'OPTIMIZE_LOCALITY_FIRST') THEN
        v_action := v_capture.recommended_action;
    ELSIF NOT v_sustained_pressure THEN
        v_action := NULL;
    ELSIF v_connection_pressure
      AND coalesce((v_capture.payload #>> '{workload,oltp_pct}')::numeric, 0) >= 50 THEN
        v_action := 'ADD_CONNECTION_POOLING';
    ELSIF v_workload_fit = 'SCALE_OUT'
      AND coalesce((v_capture.payload #>> '{workload,citus_installed}')::boolean, false) THEN
        v_action := 'ADD_CITUS_WORKERS_CANDIDATE';
    ELSIF v_workload_fit IN ('SCALE_OUT', 'CITUS_MIGRATION_CANDIDATE') THEN
        v_action := 'CITUS_MIGRATION_CANDIDATE';
    ELSIF v_connection_pressure OR v_rate_pressure OR v_infrastructure_pressure THEN
        v_action := 'SCALE_UP_CANDIDATE';
    ELSE
        v_action := 'TUNE_QUERY_OR_INDEX';
    END IF;

    -- Resolve other active actions when the current capture no longer supports
    -- them. A resolved event also enters the Azure Monitor outbox.
    FOR v_resolved IN
        WITH resolved AS (
            UPDATE fspg_ec_advisor.advisory_event
            SET
                state = 'resolved',
                resolved_at = clock_timestamp(),
                state_changed_at = clock_timestamp()
            WHERE policy_name = v_policy.policy_name
              AND server_id = v_capture.server_id
              AND database_oid = v_capture.database_oid
              AND state IN ('open', 'acknowledged', 'suppressed')
              AND (v_action IS NULL OR action <> v_action)
            RETURNING event_id, action, severity, payload
        )
        SELECT *
        FROM resolved
    LOOP
        IF v_policy.azure_monitor_enabled THEN
            PERFORM fspg_ec_advisor._enqueue_azure_monitor_outbox(
                v_resolved.event_id,
                'resolved',
                jsonb_build_object(
                    'schemaVersion', '1.0',
                    'eventType', 'fspg_ec_advisor.advisory.resolved',
                    'eventTime', clock_timestamp(),
                    'data', jsonb_build_object(
                        'eventId', v_resolved.event_id,
                        'action', v_resolved.action,
                        'severity', v_resolved.severity,
                        'captureKey', v_capture.capture_key,
                        'priorEvent', v_resolved.payload
                    )
                )
            );
        END IF;
        v_resolved_count := v_resolved_count + 1;
    END LOOP;

    IF v_action IS NULL THEN
        RETURN jsonb_build_object(
            'state', 'no_action',
            'policy', v_policy.policy_name,
            'captureKey', v_capture.capture_key,
            'confidenceAccepted', v_confidence_ok,
            'sustainedPressure', v_sustained_pressure,
            'connectionPressure', v_connection_pressure,
            'ratePressure', v_rate_pressure,
            'infrastructurePressure', v_infrastructure_pressure,
            'resolvedEvents', v_resolved_count
        );
    END IF;

    v_severity := CASE
        WHEN v_action IN ('REBALANCE_FIRST', 'OPTIMIZE_LOCALITY_FIRST') THEN 'CRITICAL'
        WHEN v_connection_pressure OR v_rate_pressure OR v_infrastructure_pressure THEN 'WARNING'
        ELSE 'INFO'
    END;
    v_dedupe_key := format(
        '%s:%s:%s:%s',
        v_capture.server_id,
        v_capture.database_oid,
        v_policy.policy_name,
        v_action
    );
    v_event_payload := jsonb_build_object(
        'schemaVersion', '1.0',
        'eventType', 'fspg_ec_advisor.advisory',
        'eventTime', clock_timestamp(),
        'data', jsonb_build_object(
            'policy', v_policy.policy_name,
            'action', v_action,
            'severity', v_severity,
            'captureKey', v_capture.capture_key,
            'capture', v_capture.payload,
            'pressure', jsonb_build_object(
                'connection', v_connection_pressure,
                'rate', v_rate_pressure,
                'infrastructure', v_infrastructure_pressure,
                'sustained', v_sustained_pressure,
                'validCaptureCount', v_valid_capture_count,
                'pressureCaptureCount', v_pressure_capture_count,
                'infrastructureSampleCount', v_infrastructure_sample_count,
                'infrastructurePressureCount', v_infrastructure_pressure_count,
                'sustainedInfrastructurePressure', v_sustained_infrastructure_pressure
            ),
            'infrastructureTelemetry', CASE
                WHEN v_has_telemetry THEN to_jsonb(v_telemetry)
                ELSE NULL
            END
        )
    );

    SELECT *
    INTO v_event
    FROM fspg_ec_advisor.advisory_event
    WHERE dedupe_key = v_dedupe_key
      AND state IN ('open', 'acknowledged', 'suppressed')
    FOR UPDATE;
    v_existing_event := FOUND;

    IF v_existing_event THEN
        IF v_event.state = 'suppressed'
           AND v_event.suppressed_until IS NOT NULL
           AND v_event.suppressed_until <= clock_timestamp() THEN
            v_event.state := 'open';
            v_event.suppressed_until := NULL;
            v_event.state_changed_at := clock_timestamp();
        END IF;

        UPDATE fspg_ec_advisor.advisory_event
        SET
            capture_key = v_capture.capture_key,
            severity = v_severity,
            state = v_event.state,
            state_changed_at = v_event.state_changed_at,
            suppressed_until = v_event.suppressed_until,
            last_seen_at = clock_timestamp(),
            occurrence_count = occurrence_count + 1,
            payload = v_event_payload
        WHERE event_id = v_event.event_id
        RETURNING * INTO v_event;
        v_event_type := 'updated';
    ELSE
        BEGIN
            INSERT INTO fspg_ec_advisor.advisory_event (
                policy_name,
                capture_key,
                server_id,
                database_oid,
                dedupe_key,
                action,
                severity,
                payload
            )
            VALUES (
                v_policy.policy_name,
                v_capture.capture_key,
                v_capture.server_id,
                v_capture.database_oid,
                v_dedupe_key,
                v_action,
                v_severity,
                v_event_payload
            )
            RETURNING * INTO v_event;
            v_event_type := 'activated';
        EXCEPTION WHEN unique_violation THEN
            SELECT *
            INTO v_event
            FROM fspg_ec_advisor.advisory_event
            WHERE dedupe_key = v_dedupe_key
              AND state IN ('open', 'acknowledged', 'suppressed')
            FOR UPDATE;

            UPDATE fspg_ec_advisor.advisory_event
            SET
                capture_key = v_capture.capture_key,
                severity = v_severity,
                last_seen_at = clock_timestamp(),
                occurrence_count = occurrence_count + 1,
                payload = v_event_payload
            WHERE event_id = v_event.event_id
            RETURNING * INTO v_event;
            v_event_type := 'updated';
        END;
    END IF;

    v_should_notify :=
        v_policy.azure_monitor_enabled
        AND v_event.state <> 'suppressed'
        AND (
            v_event.last_notified_at IS NULL
            OR clock_timestamp() - v_event.last_notified_at >= v_policy.notification_cooldown
        );

    IF v_should_notify THEN
        v_outbox_id := fspg_ec_advisor._enqueue_azure_monitor_outbox(
            v_event.event_id,
            v_event_type,
            v_event_payload
        );
        UPDATE fspg_ec_advisor.advisory_event
        SET last_notified_at = clock_timestamp()
        WHERE event_id = v_event.event_id;
    END IF;

    RETURN jsonb_build_object(
        'state', v_event.state,
        'eventId', v_event.event_id,
        'policy', v_policy.policy_name,
        'action', v_action,
        'severity', v_severity,
        'outboxId', v_outbox_id,
        'outboxQueued', v_should_notify,
        'sustainedPressure', v_sustained_pressure,
        'connectionPressure', v_connection_pressure,
        'ratePressure', v_rate_pressure,
        'infrastructurePressure', v_infrastructure_pressure,
        'sustainedInfrastructurePressure', v_sustained_infrastructure_pressure,
        'resolvedEvents', v_resolved_count
    );
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.run_advisory_cycle(
    p_policy_name text DEFAULT 'default'
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_policy fspg_ec_advisor.advisory_policy%ROWTYPE;
    v_capture jsonb;
    v_evaluation jsonb;
    v_resolved_count integer;
BEGIN
    SELECT *
    INTO v_policy
    FROM fspg_ec_advisor.advisory_policy
    WHERE policy_name = p_policy_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown advisor policy: %', p_policy_name;
    END IF;

    v_capture := fspg_ec_advisor.capture(
        v_policy.minimum_pressure_samples,
        greatest(1, ceil(extract(epoch FROM v_policy.pressure_window) / 60.0)::integer),
        v_policy.minimum_window_seconds
    );
    v_evaluation := fspg_ec_advisor.evaluate_capture(
        v_capture ->> 'capture_key',
        v_policy.policy_name
    );
    v_resolved_count := fspg_ec_advisor.resolve_stale_advisories();

    RETURN v_capture || jsonb_build_object(
        'automatedAdvisory', v_evaluation,
        'staleEventsResolved', v_resolved_count
    );
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.schedule_advisory_cycle(
    p_schedule text,
    p_policy_name text DEFAULT 'default',
    p_job_name text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_job_id bigint;
    v_job_name text;
    v_command text;
BEGIN
    IF nullif(btrim(p_schedule), '') IS NULL THEN
        RAISE EXCEPTION 'Schedule expression cannot be empty';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM fspg_ec_advisor.advisory_policy
        WHERE policy_name = p_policy_name
    ) THEN
        RAISE EXCEPTION 'Unknown advisor policy: %', p_policy_name;
    END IF;

    IF to_regprocedure('cron.schedule(text,text,text)') IS NULL THEN
        RAISE INFO 'fspg_ec_advisor could not schedule an advisory cycle because pg_cron is unavailable'
            USING HINT = 'Install and preload pg_cron, or call fspg_ec_advisor.run_advisory_cycle() from an external scheduler.';
        RETURN NULL;
    END IF;

    v_job_name := coalesce(
        nullif(btrim(p_job_name), ''),
        format('fspg_ec_advisor_%s', regexp_replace(p_policy_name, '[^a-zA-Z0-9_]+', '_', 'g'))
    );
    v_command := format('SELECT fspg_ec_advisor.run_advisory_cycle(%L);', p_policy_name);

    EXECUTE 'SELECT cron.schedule($1, $2, $3)'
    INTO v_job_id
    USING v_job_name, p_schedule, v_command;

    RETURN v_job_id;
END
$plpgsql$;

CREATE FUNCTION fspg_ec_advisor.unschedule_advisory_cycle(p_job_id bigint)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $plpgsql$
DECLARE
    v_unscheduled boolean;
BEGIN
    IF p_job_id IS NULL OR p_job_id <= 0 THEN
        RAISE EXCEPTION 'pg_cron job id must be positive';
    END IF;

    IF to_regprocedure('cron.unschedule(bigint)') IS NULL THEN
        RAISE INFO 'fspg_ec_advisor could not unschedule an advisory cycle because pg_cron is unavailable'
            USING HINT = 'Use the scheduler that owns the advisory-cycle invocation.';
        RETURN false;
    END IF;

    EXECUTE 'SELECT cron.unschedule($1)'
    INTO v_unscheduled
    USING p_job_id;

    RETURN coalesce(v_unscheduled, false);
END
$plpgsql$;