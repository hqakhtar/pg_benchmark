\set ECHO none

CREATE TEMP TABLE fspg_ec_advisor_config_assertions (
    assertion_name text PRIMARY KEY,
    passed boolean NOT NULL
);

DO $plpgsql$
BEGIN
    BEGIN
        EXECUTE 'CREATE EXTENSION fspg_ec_advisor';
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('missing_pg_stat_statements_dependency_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'missing_pg_stat_statements_dependency_rejected',
            SQLERRM LIKE '%required extension "pg_stat_statements" is not installed%'
        );
    END;
END
$plpgsql$;

CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION fspg_ec_advisor;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'extension_version_is_1_0',
    (SELECT extversion = '1.0' FROM pg_extension WHERE extname = 'fspg_ec_advisor');

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'latest_advice_is_null_before_capture',
    fspg_ec_advisor.latest_advice() IS NULL;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'optional_wait_metrics_are_empty_without_view',
    NOT EXISTS (SELECT 1 FROM fspg_ec_advisor._wait_metrics());

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'optional_cross_node_metrics_are_empty_without_citus',
    NOT EXISTS (SELECT 1 FROM fspg_ec_advisor._cross_node_metrics());

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'cluster_fairness_is_null_without_citus',
    fspg_ec_advisor.cluster_fairness() IS NULL;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'resource_snapshot_has_connection_status',
    fspg_ec_advisor.resource_snapshot() ? 'resource_pressure_status';

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'default_policy_is_enabled',
    (SELECT enabled FROM fspg_ec_advisor.advisory_policy WHERE policy_name = 'default');

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_guc_is_unset',
    current_setting('fspg_ec_advisor.reset_stats', true) IS NULL;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_guc_unset_returns_false',
    NOT fspg_ec_advisor.reset_stats();

SET fspg_ec_advisor.reset_stats = off;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_guc_explicit_off_returns_false',
    current_setting('fspg_ec_advisor.reset_stats', true) = 'off'
    AND NOT fspg_ec_advisor.reset_stats();

SET fspg_ec_advisor.reset_stats = 'not-a-boolean';

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_guc_invalid_returns_false',
    NOT fspg_ec_advisor.reset_stats();

SET fspg_ec_advisor.reset_stats = on;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_guc_explicit_on_returns_true',
    fspg_ec_advisor.reset_stats();

RESET fspg_ec_advisor.reset_stats;

DO $plpgsql$
BEGIN
    BEGIN
        PERFORM fspg_ec_advisor.capture(0, 60, 0);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_capture_samples_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_capture_samples_rejected',
            SQLERRM = 'pressure_samples and pressure_window_minutes must be positive; min_window_seconds cannot be negative'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.capture(1, 0, 0);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_capture_window_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_capture_window_rejected',
            SQLERRM = 'pressure_samples and pressure_window_minutes must be positive; min_window_seconds cannot be negative'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.capture(1, 60, -1);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_capture_interval_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_capture_interval_rejected',
            SQLERRM = 'pressure_samples and pressure_window_minutes must be positive; min_window_seconds cannot be negative'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.ingest_infrastructure_telemetry('', p_cpu_pct => 10);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('empty_telemetry_source_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'empty_telemetry_source_rejected',
            SQLERRM = 'Telemetry source cannot be empty'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.ingest_infrastructure_telemetry('pg_regress', p_cpu_pct => 101);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_telemetry_value_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_telemetry_value_rejected',
            SQLERRM = 'Infrastructure telemetry contains an invalid metric value'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.ingest_infrastructure_telemetry(
            'pg_regress',
            p_collected_at => clock_timestamp() + interval '6 minutes'
        );
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('future_telemetry_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'future_telemetry_rejected',
            SQLERRM = 'Telemetry collection timestamp cannot be more than five minutes in the future'
        );
    END;

    BEGIN
        UPDATE fspg_ec_advisor.advisory_policy
        SET pressure_window = interval '0 seconds'
        WHERE policy_name = 'default';
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_policy_window_rejected', false);
    EXCEPTION WHEN check_violation THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_policy_window_rejected', true);
    END;

    BEGIN
        UPDATE fspg_ec_advisor.advisory_policy
        SET application_latency_ms = -1
        WHERE policy_name = 'default';
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_policy_latency_rejected', false);
    EXCEPTION WHEN check_violation THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_policy_latency_rejected', true);
    END;

    BEGIN
        PERFORM fspg_ec_advisor.claim_azure_monitor_outbox(0, 60);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_outbox_claim_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_outbox_claim_rejected',
            SQLERRM = 'Outbox limit and lease duration must be positive'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.complete_azure_monitor_delivery(1, false, 'invalid max', 0);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_delivery_attempts_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_delivery_attempts_rejected',
            SQLERRM = 'Maximum delivery attempts must be positive'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.suppress_advisory_event(1, 0);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_suppression_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_suppression_rejected',
            SQLERRM = 'Suppression duration must be positive'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.run_advisory_cycle('missing');
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('unknown_policy_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'unknown_policy_rejected',
            SQLERRM = 'Unknown advisor policy: missing'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.schedule_advisory_cycle('', 'default');
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('empty_schedule_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'empty_schedule_rejected',
            SQLERRM = 'Schedule expression cannot be empty'
        );
    END;

    BEGIN
        PERFORM fspg_ec_advisor.unschedule_advisory_cycle(0);
        INSERT INTO fspg_ec_advisor_config_assertions VALUES ('invalid_schedule_job_id_rejected', false);
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO fspg_ec_advisor_config_assertions VALUES (
            'invalid_schedule_job_id_rejected',
            SQLERRM = 'pg_cron job id must be positive'
        );
    END;
END
$plpgsql$;

SELECT fspg_ec_advisor.schedule_advisory_cycle('* * * * *') IS NULL AS scheduler_unavailable_returns_null;
SELECT NOT fspg_ec_advisor.unschedule_advisory_cycle(1) AS scheduler_unavailable_returns_false;

UPDATE fspg_ec_advisor.advisory_policy
SET enabled = false
WHERE policy_name = 'default';

CREATE TEMP TABLE fspg_ec_advisor_disabled_cycle AS
SELECT fspg_ec_advisor.run_advisory_cycle('default') AS payload;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'disabled_policy_returns_disabled_state',
    payload #>> '{automatedAdvisory,state}' = 'disabled'
FROM fspg_ec_advisor_disabled_cycle;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'disabled_policy_creates_no_event',
    NOT EXISTS (SELECT 1 FROM fspg_ec_advisor.advisory_event);

UPDATE fspg_ec_advisor.advisory_policy
SET
    enabled = true,
    minimum_confidence = 'LOW',
    minimum_pressure_samples = 1,
    minimum_window_seconds = 0,
    pressure_window = interval '60 minutes',
    telemetry_max_age = interval '15 minutes',
    cpu_pct = 90,
    notification_cooldown = interval '0 seconds',
    auto_resolve_after = interval '1 hour',
    azure_monitor_enabled = false
WHERE policy_name = 'default';

SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source => 'fresh-config-test',
    p_cpu_pct => 99
) > 0 AS fresh_telemetry_ingested \gset

CREATE TEMP TABLE fspg_ec_advisor_no_delivery_cycle AS
SELECT fspg_ec_advisor.run_advisory_cycle('default') AS payload;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'fresh_telemetry_is_ingested',
    :'fresh_telemetry_ingested'::boolean;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'azure_delivery_disabled_creates_event_without_outbox',
    payload #>> '{automatedAdvisory,eventId}' IS NOT NULL
    AND NOT coalesce((payload #>> '{automatedAdvisory,outboxQueued}')::boolean, false)
    AND NOT EXISTS (SELECT 1 FROM fspg_ec_advisor.azure_monitor_outbox)
FROM fspg_ec_advisor_no_delivery_cycle;

UPDATE fspg_ec_advisor.advisory_policy
SET azure_monitor_enabled = true
WHERE policy_name = 'default';

CREATE TEMP TABLE fspg_ec_advisor_enabled_evaluation AS
SELECT fspg_ec_advisor.evaluate_capture(
    payload ->> 'capture_key',
    'default'
) AS payload
FROM fspg_ec_advisor_no_delivery_cycle;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'azure_delivery_enabled_queues_outbox',
    coalesce((payload ->> 'outboxQueued')::boolean, false)
    AND payload ->> 'outboxId' IS NOT NULL
FROM fspg_ec_advisor_enabled_evaluation;

CREATE TEMP TABLE fspg_ec_advisor_claimed_delivery AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 1);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'outbox_claims_enabled_delivery',
    count(*) = 1
    AND bool_and(event_type = 'updated')
    AND bool_and(attempts = 1)
FROM fspg_ec_advisor_claimed_delivery;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'complete_unclaimed_delivery_returns_false',
    NOT fspg_ec_advisor.complete_azure_monitor_delivery(999999, true);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'failed_delivery_requeues_before_limit',
    fspg_ec_advisor.complete_azure_monitor_delivery(
        (SELECT outbox_id FROM fspg_ec_advisor_claimed_delivery LIMIT 1),
        false,
        'configuration retry',
        2
    );

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'failed_delivery_is_pending_with_error',
    status = 'pending'
    AND attempts = 1
    AND last_error = 'configuration retry'
FROM fspg_ec_advisor.azure_monitor_outbox
WHERE outbox_id = (SELECT outbox_id FROM fspg_ec_advisor_claimed_delivery LIMIT 1);

UPDATE fspg_ec_advisor.azure_monitor_outbox
SET available_at = clock_timestamp() - interval '1 second'
WHERE outbox_id = (SELECT outbox_id FROM fspg_ec_advisor_claimed_delivery LIMIT 1);

CREATE TEMP TABLE fspg_ec_advisor_reclaimed_delivery AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 1);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'expired_or_requeued_delivery_is_reclaimed',
    count(*) = 1
    AND bool_and(attempts = 2)
FROM fspg_ec_advisor_reclaimed_delivery;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'failed_delivery_reaches_terminal_state_at_limit',
    fspg_ec_advisor.complete_azure_monitor_delivery(
        (SELECT outbox_id FROM fspg_ec_advisor_reclaimed_delivery LIMIT 1),
        false,
        'configuration terminal failure',
        2
    );

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'terminal_delivery_is_failed',
    status = 'failed'
FROM fspg_ec_advisor.azure_monitor_outbox
WHERE outbox_id = (SELECT outbox_id FROM fspg_ec_advisor_reclaimed_delivery LIMIT 1);

CREATE TEMP TABLE fspg_ec_advisor_successful_evaluation AS
SELECT fspg_ec_advisor.evaluate_capture(
    payload ->> 'capture_key',
    'default'
) AS payload
FROM fspg_ec_advisor_no_delivery_cycle;

CREATE TEMP TABLE fspg_ec_advisor_successful_delivery AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 60);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'new_delivery_is_claimed_after_terminal_failure',
    count(*) = 1
    AND bool_and(event_type = 'updated')
FROM fspg_ec_advisor_successful_delivery;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'successful_delivery_is_acknowledged',
    fspg_ec_advisor.complete_azure_monitor_delivery(
        (SELECT outbox_id FROM fspg_ec_advisor_successful_delivery LIMIT 1),
        true
    );

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'successful_delivery_has_delivered_state',
    status = 'delivered'
FROM fspg_ec_advisor.azure_monitor_outbox
WHERE outbox_id = (SELECT outbox_id FROM fspg_ec_advisor_successful_delivery LIMIT 1);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'unknown_event_acknowledgement_returns_false',
    NOT fspg_ec_advisor.acknowledge_advisory_event(999999);

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'event_acknowledgement_succeeds',
    fspg_ec_advisor.acknowledge_advisory_event(
        (SELECT event_id FROM fspg_ec_advisor_successful_delivery LIMIT 1)
    );

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'event_suppression_succeeds',
    fspg_ec_advisor.suppress_advisory_event(
        (SELECT event_id FROM fspg_ec_advisor_successful_delivery LIMIT 1),
        60
    );

CREATE TEMP TABLE fspg_ec_advisor_suppressed_evaluation AS
SELECT fspg_ec_advisor.evaluate_capture(
    payload ->> 'capture_key',
    'default'
) AS payload
FROM fspg_ec_advisor_no_delivery_cycle;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'suppressed_event_does_not_queue_delivery',
    NOT coalesce((payload ->> 'outboxQueued')::boolean, false)
FROM fspg_ec_advisor_suppressed_evaluation;

UPDATE fspg_ec_advisor.advisory_event
SET
    suppressed_until = clock_timestamp() - interval '1 second',
    last_seen_at = clock_timestamp() - interval '2 hours'
WHERE event_id = (SELECT event_id FROM fspg_ec_advisor_successful_delivery LIMIT 1);

CREATE TEMP TABLE fspg_ec_advisor_expired_suppression_evaluation AS
SELECT fspg_ec_advisor.evaluate_capture(
    payload ->> 'capture_key',
    'default'
) AS payload
FROM fspg_ec_advisor_no_delivery_cycle;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'expired_suppression_reopens_and_queues_delivery',
    payload ->> 'state' = 'open'
    AND coalesce((payload ->> 'outboxQueued')::boolean, false)
FROM fspg_ec_advisor_expired_suppression_evaluation;

CREATE TEMP TABLE fspg_ec_advisor_expired_suppression_delivery AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 60);

SELECT fspg_ec_advisor.complete_azure_monitor_delivery(
    (SELECT outbox_id FROM fspg_ec_advisor_expired_suppression_delivery LIMIT 1),
    true
);

TRUNCATE fspg_ec_advisor.infrastructure_telemetry;

UPDATE fspg_ec_advisor.advisory_policy
SET
    telemetry_max_age = interval '1 minute',
    azure_monitor_enabled = false
WHERE policy_name = 'default';

SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source => 'stale-config-test',
    p_collected_at => clock_timestamp() - interval '30 minutes',
    p_cpu_pct => 99
) > 0 AS stale_telemetry_ingested \gset

CREATE TEMP TABLE fspg_ec_advisor_stale_telemetry_cycle AS
SELECT fspg_ec_advisor.run_advisory_cycle('default') AS payload;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'stale_telemetry_is_ingested',
    :'stale_telemetry_ingested'::boolean;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'stale_telemetry_does_not_create_new_action',
    payload #>> '{automatedAdvisory,state}' = 'no_action'
    AND NOT coalesce((payload #>> '{automatedAdvisory,infrastructurePressure}')::boolean, false)
    AND NOT coalesce((payload #>> '{automatedAdvisory,sustainedInfrastructurePressure}')::boolean, false)
FROM fspg_ec_advisor_stale_telemetry_cycle;

UPDATE fspg_ec_advisor.advisory_policy
SET
    enabled = true,
    minimum_confidence = 'LOW',
    minimum_pressure_samples = 1,
    minimum_window_seconds = 0,
    telemetry_max_age = interval '15 minutes',
    azure_monitor_enabled = false
WHERE policy_name = 'default';

SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source => 'reset-safety-test',
    p_cpu_pct => 99
) > 0 AS reset_safety_telemetry_ingested \gset

SET fspg_ec_advisor.reset_stats = on;

SELECT fspg_ec_advisor.capture_and_reset(
    p_pressure_samples => 1,
    p_pressure_window_minutes => 60,
    p_min_window_seconds => 0
) ? 'recommendation' AS reset_safety_capture_created;

SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source => 'reset-safety-test',
    p_cpu_pct => 99
) > 0 AS reset_safety_second_telemetry_ingested \gset

CREATE TEMP TABLE fspg_ec_advisor_reset_safety_cycle AS
SELECT fspg_ec_advisor.run_advisory_cycle('default') AS payload;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'reset_safety_telemetry_is_ingested',
    :'reset_safety_telemetry_ingested'::boolean
    AND :'reset_safety_second_telemetry_ingested'::boolean;

INSERT INTO fspg_ec_advisor_config_assertions
SELECT
    'statistics_reset_blocks_automated_action',
    payload #>> '{window,blocker}' = 'STATISTICS_RESET'
    AND payload #>> '{automatedAdvisory,state}' = 'no_action'
FROM fspg_ec_advisor_reset_safety_cycle;

RESET fspg_ec_advisor.reset_stats;

SELECT assertion_name, passed
FROM fspg_ec_advisor_config_assertions
ORDER BY assertion_name;

DROP EXTENSION fspg_ec_advisor CASCADE;
DROP EXTENSION pg_stat_statements;
\echo fspg_ec_advisor_config_regression_complete