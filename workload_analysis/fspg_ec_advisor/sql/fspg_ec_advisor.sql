\set ECHO none

CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION fspg_ec_advisor;

SELECT extname, extnamespace::regnamespace
FROM pg_extension
WHERE extname = 'fspg_ec_advisor';

SELECT 1 AS fspg_ec_advisor_regression_probe;

SELECT
    count(*) > 0 AS query_scores_return_rows,
    bool_and(queryid IS NOT NULL) AS query_scores_have_queryids,
    bool_and(total_exec_ms >= 0) AS query_scores_have_execution_time
FROM fspg_ec_advisor.query_scores();

CREATE TEMP TABLE fspg_ec_advisor_capture_assertions (
    payload jsonb NOT NULL
);

INSERT INTO fspg_ec_advisor_capture_assertions (payload)
SELECT fspg_ec_advisor.capture(
    p_pressure_samples => 1,
    p_pressure_window_minutes => 60,
    p_min_window_seconds => 0
);

SELECT
    payload ? 'capture_key' AS has_capture_key,
    payload ? 'workload' AS has_workload,
    payload ? 'resource' AS has_resource,
    payload ? 'window' AS has_window,
    payload ? 'recommendation' AS has_recommendation,
    payload -> 'recommendation' ? 'action' AS has_action,
    payload -> 'recommendation' ? 'confidence' AS has_confidence
FROM fspg_ec_advisor_capture_assertions;

SELECT
    count(*) = 1 AS history_has_capture,
    bool_and(recommended_action IS NOT NULL) AS history_has_action,
    bool_and(payload ? 'recommendation') AS history_has_payload
FROM fspg_ec_advisor.capture_history;

SELECT fspg_ec_advisor.record_feedback(
    payload ->> 'capture_key',
    payload -> 'recommendation' ->> 'action',
    'improved',
    'pg_regress feedback'
) > 0 AS feedback_recorded
FROM fspg_ec_advisor_capture_assertions;

SELECT
    count(*) = 1 AS calibration_has_group,
    max(feedback_count) = 1 AS calibration_has_feedback,
    max(positive_outcome_count) = 1 AS calibration_has_positive_outcome
FROM fspg_ec_advisor.recommendation_calibration;

UPDATE fspg_ec_advisor.advisory_policy
SET
    minimum_confidence = 'LOW',
    minimum_pressure_samples = 1,
    minimum_window_seconds = 0,
    cpu_pct = 90,
    notification_cooldown = interval '0 seconds'
WHERE policy_name = 'default';

SELECT fspg_ec_advisor.ingest_infrastructure_telemetry(
    p_source => 'pg_regress',
    p_cpu_pct => 99
) > 0 AS infrastructure_telemetry_ingested;

CREATE TEMP TABLE fspg_ec_advisor_event_assertions (
    payload jsonb NOT NULL
);

INSERT INTO fspg_ec_advisor_event_assertions (payload)
SELECT fspg_ec_advisor.run_advisory_cycle('default');

SELECT
    payload ? 'automatedAdvisory' AS cycle_has_automation,
    payload -> 'automatedAdvisory' ? 'eventId' AS cycle_has_event,
    payload -> 'automatedAdvisory' ? 'outboxId' AS cycle_queues_outbox
FROM fspg_ec_advisor_event_assertions;

CREATE TEMP TABLE fspg_ec_advisor_outbox_assertions AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 60);

SELECT
    count(*) = 1 AS outbox_has_event,
    bool_and(event_type = 'activated') AS outbox_is_activation,
    bool_and(attempts = 1) AS outbox_claimed_once
FROM fspg_ec_advisor_outbox_assertions;

SELECT fspg_ec_advisor.complete_azure_monitor_delivery(
    (SELECT outbox_id FROM fspg_ec_advisor_outbox_assertions LIMIT 1),
    true
) AS azure_monitor_delivery_completed;

SELECT fspg_ec_advisor.acknowledge_advisory_event(
    (SELECT event_id FROM fspg_ec_advisor_outbox_assertions LIMIT 1)
) AS advisory_event_acknowledged;

SELECT fspg_ec_advisor.suppress_advisory_event(
    (SELECT event_id FROM fspg_ec_advisor_outbox_assertions LIMIT 1),
    60
) AS advisory_event_suppressed;

UPDATE fspg_ec_advisor.advisory_event
SET last_seen_at = clock_timestamp() - interval '3 hours'
WHERE event_id = (SELECT event_id FROM fspg_ec_advisor_outbox_assertions LIMIT 1);

SELECT fspg_ec_advisor.resolve_stale_advisories() = 1 AS stale_event_resolved;

CREATE TEMP TABLE fspg_ec_advisor_resolution_outbox_assertions AS
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(10, 60);

SELECT
    count(*) = 1 AS resolution_outbox_has_event,
    bool_and(event_type = 'resolved') AS resolution_outbox_is_resolved
FROM fspg_ec_advisor_resolution_outbox_assertions;

SELECT fspg_ec_advisor.complete_azure_monitor_delivery(
    (SELECT outbox_id FROM fspg_ec_advisor_resolution_outbox_assertions LIMIT 1),
    false,
    'pg_regress retry test',
    2
) AS azure_monitor_delivery_requeued;

SELECT
    count(*) = 1 AS retry_outbox_has_event,
    bool_and(status = 'pending') AS retry_outbox_is_pending,
    bool_and(last_error = 'pg_regress retry test') AS retry_outbox_has_error
FROM fspg_ec_advisor.azure_monitor_outbox
WHERE outbox_id = (SELECT outbox_id FROM fspg_ec_advisor_resolution_outbox_assertions LIMIT 1);

SELECT fspg_ec_advisor.reset_stats() AS reset_when_disabled;

SET fspg_ec_advisor.reset_stats = on;

SELECT fspg_ec_advisor.capture_and_reset(
    p_pressure_samples => 1,
    p_pressure_window_minutes => 60,
    p_min_window_seconds => 0
) ? 'recommendation' AS capture_and_reset_returns_capture;

SELECT
    fspg_ec_advisor.capture(
        p_pressure_samples => 1,
        p_pressure_window_minutes => 60,
        p_min_window_seconds => 0
    ) -> 'window' ->> 'blocker' AS reset_blocker;

SELECT
    fspg_ec_advisor.latest_advice() -> 'recommendation' ->> 'action'
        AS latest_action_is_available;

DROP EXTENSION fspg_ec_advisor CASCADE;
DROP EXTENSION pg_stat_statements;
\echo fspg_ec_advisor_regression_complete