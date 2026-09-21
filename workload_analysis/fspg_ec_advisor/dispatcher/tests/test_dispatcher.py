from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fspg_ec_advisor_azure_monitor import (  # noqa: E402
    AdvisoryDispatcher,
    ConfigurationError,
    DispatcherConfig,
    OutboxRecord,
    to_log_record,
)


class FakeRepository:
    def __init__(self, records: list[OutboxRecord]) -> None:
        self.records = records
        self.claim_calls: list[tuple[int, int]] = []
        self.completions: list[tuple[int, bool, str | None, int]] = []

    def claim(self, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        self.claim_calls.append((limit, lease_seconds))
        records, self.records = self.records, []
        return records

    def complete(self, outbox_id: int, success: bool, error: str | None, max_attempts: int) -> bool:
        self.completions.append((outbox_id, success, error, max_attempts))
        return True


class FakeSender:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.uploads: list[list[dict[str, object]]] = []

    def upload(self, records: list[dict[str, object]]) -> None:
        if self.error is not None:
            raise self.error
        self.uploads.append(records)


def config() -> DispatcherConfig:
    return DispatcherConfig(
        dsn="postgresql://dispatcher@localhost/advisor?sslmode=require",
        dcr_endpoint="https://example.ingest.monitor.azure.com",
        dcr_immutable_id="dcr-test",
        stream_name="Custom-FspgEcAdvisorEvents_CL",
        batch_size=2,
        lease_seconds=60,
        max_attempts=3,
        poll_seconds=1,
    )


def record(outbox_id: int = 1) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=outbox_id,
        event_id=42,
        event_type="activated",
        attempts=1,
        payload={
            "schemaVersion": "1.0",
            "eventTime": "2026-09-21T00:00:00Z",
            "data": {"action": "SCALE_UP_CANDIDATE", "severity": "WARNING", "captureKey": "capture"},
        },
    )


class DispatcherConfigTests(unittest.TestCase):
    def test_configuration_requires_azure_and_database_settings(self) -> None:
        with self.assertRaises(ConfigurationError):
            DispatcherConfig.from_environment({})

    def test_configuration_accepts_explicit_settings(self) -> None:
        parsed = DispatcherConfig.from_environment(
            {
                "FSPG_EC_ADVISOR_DSN": "postgresql://dispatcher@localhost/advisor?sslmode=require",
                "FSPG_EC_ADVISOR_DCR_ENDPOINT": "https://example.ingest.monitor.azure.com",
                "FSPG_EC_ADVISOR_DCR_IMMUTABLE_ID": "dcr-test",
                "FSPG_EC_ADVISOR_DCR_STREAM_NAME": "Custom-FspgEcAdvisorEvents_CL",
                "FSPG_EC_ADVISOR_BATCH_SIZE": "10",
                "FSPG_EC_ADVISOR_LEASE_SECONDS": "90",
                "FSPG_EC_ADVISOR_MAX_ATTEMPTS": "4",
                "FSPG_EC_ADVISOR_POLL_SECONDS": "2",
                "FSPG_EC_ADVISOR_MANAGED_IDENTITY_CLIENT_ID": "client-id",
            }
        )
        self.assertEqual(parsed.batch_size, 10)
        self.assertEqual(parsed.lease_seconds, 90)
        self.assertEqual(parsed.managed_identity_client_id, "client-id")

    def test_configuration_rejects_non_positive_numbers(self) -> None:
        environment = {
            "FSPG_EC_ADVISOR_DSN": "postgresql://dispatcher@localhost/advisor?sslmode=require",
            "FSPG_EC_ADVISOR_DCR_ENDPOINT": "https://example.ingest.monitor.azure.com",
            "FSPG_EC_ADVISOR_DCR_IMMUTABLE_ID": "dcr-test",
            "FSPG_EC_ADVISOR_DCR_STREAM_NAME": "Custom-FspgEcAdvisorEvents_CL",
            "FSPG_EC_ADVISOR_BATCH_SIZE": "0",
        }
        with self.assertRaises(ConfigurationError):
            DispatcherConfig.from_environment(environment)

    def test_configuration_rejects_insecure_endpoints_and_dsns(self) -> None:
        environment = {
            "FSPG_EC_ADVISOR_DSN": "postgresql://dispatcher@localhost/advisor",
            "FSPG_EC_ADVISOR_DCR_ENDPOINT": "http://example.ingest.monitor.azure.com",
            "FSPG_EC_ADVISOR_DCR_IMMUTABLE_ID": "dcr-test",
            "FSPG_EC_ADVISOR_DCR_STREAM_NAME": "Custom-FspgEcAdvisorEvents_CL",
        }
        with self.assertRaises(ConfigurationError):
            DispatcherConfig.from_environment(environment)


class DispatcherFlowTests(unittest.TestCase):
    def test_empty_outbox_does_not_send_or_complete(self) -> None:
        repository = FakeRepository([])
        sender = FakeSender()
        processed = AdvisoryDispatcher(repository, sender, config()).run_once()
        self.assertEqual(processed, 0)
        self.assertEqual(sender.uploads, [])
        self.assertEqual(repository.completions, [])

    def test_successful_batch_upload_marks_all_records_delivered(self) -> None:
        repository = FakeRepository([record(1), record(2)])
        sender = FakeSender()
        processed = AdvisoryDispatcher(repository, sender, config()).run_once()
        self.assertEqual(processed, 2)
        self.assertEqual(repository.claim_calls, [(2, 60)])
        self.assertEqual([entry[:2] for entry in repository.completions], [(1, True), (2, True)])
        self.assertEqual(sender.uploads[0][0]["Action"], "SCALE_UP_CANDIDATE")

    def test_failed_batch_upload_requeues_all_records(self) -> None:
        repository = FakeRepository([record(3), record(4)])
        sender = FakeSender(RuntimeError("HTTP 429"))
        processed = AdvisoryDispatcher(repository, sender, config()).run_once()
        self.assertEqual(processed, 2)
        self.assertEqual([entry[:2] for entry in repository.completions], [(3, False), (4, False)])
        self.assertTrue(all("RuntimeError: HTTP 429" in str(entry[2]) for entry in repository.completions))

    def test_log_record_preserves_envelope_metadata(self) -> None:
        payload = to_log_record(record())
        self.assertEqual(payload["OutboxId"], 1)
        self.assertEqual(payload["EventId"], 42)
        self.assertEqual(payload["Severity"], "WARNING")
        self.assertIn('"schemaVersion":"1.0"', str(payload["Payload"]))


if __name__ == "__main__":
    unittest.main()