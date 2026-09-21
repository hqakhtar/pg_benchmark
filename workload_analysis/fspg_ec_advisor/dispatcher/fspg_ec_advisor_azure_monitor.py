#!/usr/bin/env python3
"""Deliver FSPG EC Advisor outbox events to Azure Monitor Logs Ingestion."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, Sequence


LOGGER = logging.getLogger("fspg_ec_advisor.azure_monitor")


class ConfigurationError(ValueError):
    """Raised when required dispatcher configuration is absent or invalid."""


@dataclass(frozen=True)
class DispatcherConfig:
    dsn: str
    dcr_endpoint: str
    dcr_immutable_id: str
    stream_name: str
    batch_size: int = 50
    lease_seconds: int = 300
    max_attempts: int = 8
    poll_seconds: int = 15
    managed_identity_client_id: str | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "DispatcherConfig":
        values = environment if environment is not None else os.environ

        def required(name: str) -> str:
            value = values.get(name, "").strip()
            if not value:
                raise ConfigurationError(f"{name} must be configured")
            return value

        def positive_integer(name: str, default: int) -> int:
            value = values.get(name, str(default)).strip()
            try:
                number = int(value)
            except ValueError as error:
                raise ConfigurationError(f"{name} must be a positive integer") from error
            if number <= 0:
                raise ConfigurationError(f"{name} must be a positive integer")
            return number

        dsn = required("FSPG_EC_ADVISOR_DSN")
        if "sslmode=require" not in dsn and "sslmode=verify-ca" not in dsn and "sslmode=verify-full" not in dsn:
            raise ConfigurationError("FSPG_EC_ADVISOR_DSN must require TLS with sslmode=require, verify-ca, or verify-full")

        dcr_endpoint = required("FSPG_EC_ADVISOR_DCR_ENDPOINT")
        if not dcr_endpoint.startswith("https://"):
            raise ConfigurationError("FSPG_EC_ADVISOR_DCR_ENDPOINT must use HTTPS")

        return cls(
            dsn=dsn,
            dcr_endpoint=dcr_endpoint,
            dcr_immutable_id=required("FSPG_EC_ADVISOR_DCR_IMMUTABLE_ID"),
            stream_name=required("FSPG_EC_ADVISOR_DCR_STREAM_NAME"),
            batch_size=positive_integer("FSPG_EC_ADVISOR_BATCH_SIZE", 50),
            lease_seconds=positive_integer("FSPG_EC_ADVISOR_LEASE_SECONDS", 300),
            max_attempts=positive_integer("FSPG_EC_ADVISOR_MAX_ATTEMPTS", 8),
            poll_seconds=positive_integer("FSPG_EC_ADVISOR_POLL_SECONDS", 15),
            managed_identity_client_id=values.get("FSPG_EC_ADVISOR_MANAGED_IDENTITY_CLIENT_ID") or None,
        )


@dataclass(frozen=True)
class OutboxRecord:
    outbox_id: int
    event_id: int
    event_type: str
    payload: Mapping[str, Any]
    attempts: int


class OutboxRepository(Protocol):
    def claim(self, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        """Claim durable outbox work for a bounded delivery lease."""

    def complete(self, outbox_id: int, success: bool, error: str | None, max_attempts: int) -> bool:
        """Mark a claimed event delivered or schedule its retry."""


class AzureMonitorSender(Protocol):
    def upload(self, records: Sequence[Mapping[str, Any]]) -> None:
        """Upload a batch of normalized advisory records."""


class PsycopgOutboxRepository:
    """A short-lived, parameterized PostgreSQL connection per outbox operation."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def claim(self, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        import psycopg
        from psycopg.rows import dict_row

        query = """
            SELECT outbox_id, event_id, event_type, payload, attempts
            FROM fspg_ec_advisor.claim_azure_monitor_outbox(%s, %s)
        """
        with psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit, lease_seconds))
                rows = cursor.fetchall()

        return [
            OutboxRecord(
                outbox_id=int(row["outbox_id"]),
                event_id=int(row["event_id"]),
                event_type=str(row["event_type"]),
                payload=_as_mapping(row["payload"]),
                attempts=int(row["attempts"]),
            )
            for row in rows
        ]

    def complete(self, outbox_id: int, success: bool, error: str | None, max_attempts: int) -> bool:
        import psycopg

        query = """
            SELECT fspg_ec_advisor.complete_azure_monitor_delivery(%s, %s, %s, %s)
        """
        with psycopg.connect(self._dsn, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (outbox_id, success, error, max_attempts))
                row = cursor.fetchone()
        return bool(row and row[0])


class LogsIngestionSender:
    """Azure Monitor Logs Ingestion sender authenticated only by Managed Identity."""

    def __init__(self, endpoint: str, immutable_rule_id: str, stream_name: str, client_id: str | None = None) -> None:
        from azure.identity import ManagedIdentityCredential
        from azure.monitor.ingestion import LogsIngestionClient

        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
        self._client = LogsIngestionClient(endpoint=endpoint, credential=credential)
        self._immutable_rule_id = immutable_rule_id
        self._stream_name = stream_name

    def upload(self, records: Sequence[Mapping[str, Any]]) -> None:
        self._client.upload(
            rule_id=self._immutable_rule_id,
            stream_name=self._stream_name,
            logs=list(records),
        )


class AdvisoryDispatcher:
    def __init__(
        self,
        repository: OutboxRepository,
        sender: AzureMonitorSender,
        config: DispatcherConfig,
    ) -> None:
        self._repository = repository
        self._sender = sender
        self._config = config

    def run_once(self) -> int:
        records = self._repository.claim(self._config.batch_size, self._config.lease_seconds)
        if not records:
            return 0

        try:
            self._sender.upload([to_log_record(record) for record in records])
        except Exception as error:  # Azure SDK errors are intentionally retried by the outbox.
            message = _safe_error_message(error)
            LOGGER.exception("Azure Monitor batch delivery failed for %d outbox record(s)", len(records))
            for record in records:
                self._repository.complete(record.outbox_id, False, message, self._config.max_attempts)
            return len(records)

        for record in records:
            self._repository.complete(record.outbox_id, True, None, self._config.max_attempts)
        return len(records)

    def run_forever(self, stop_requested: "StopRequested") -> None:
        while not stop_requested.value:
            processed = self.run_once()
            if processed == 0:
                stop_requested.wait(self._config.poll_seconds)


class StopRequested:
    def __init__(self) -> None:
        self.value = False

    def request(self, *_: object) -> None:
        self.value = True

    def wait(self, seconds: int) -> None:
        for _ in range(seconds):
            if self.value:
                return
            time.sleep(1)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, Mapping):
            return parsed
    raise ValueError("Outbox payload is not a JSON object")


def _safe_error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {str(error)[:512]}"


def to_log_record(record: OutboxRecord) -> dict[str, Any]:
    payload = dict(record.payload)
    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
    event_time = payload.get("eventTime") or datetime.now(timezone.utc).isoformat()
    return {
        "TimeGenerated": event_time,
        "OutboxId": record.outbox_id,
        "EventId": record.event_id,
        "EventType": record.event_type,
        "Action": data.get("action"),
        "Severity": data.get("severity"),
        "CaptureKey": data.get("captureKey"),
        "SchemaVersion": payload.get("schemaVersion", "1.0"),
        "Payload": json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
    }


def build_dispatcher(config: DispatcherConfig) -> AdvisoryDispatcher:
    return AdvisoryDispatcher(
        repository=PsycopgOutboxRepository(config.dsn),
        sender=LogsIngestionSender(
            endpoint=config.dcr_endpoint,
            immutable_rule_id=config.dcr_immutable_id,
            stream_name=config.stream_name,
            client_id=config.managed_identity_client_id,
        ),
        config=config,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="claim and deliver one outbox batch")
    parser.add_argument("--log-level", default="INFO", help="Python log level")
    arguments = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, arguments.log_level.upper(), logging.INFO))
    try:
        config = DispatcherConfig.from_environment()
    except ConfigurationError as error:
        LOGGER.error("Invalid dispatcher configuration: %s", error)
        return 2

    dispatcher = build_dispatcher(config)
    if arguments.once:
        dispatcher.run_once()
        return 0

    stop_requested = StopRequested()
    signal.signal(signal.SIGINT, stop_requested.request)
    signal.signal(signal.SIGTERM, stop_requested.request)
    dispatcher.run_forever(stop_requested)
    return 0


if __name__ == "__main__":
    sys.exit(main())