# Azure Monitor Dispatcher

This optional worker is outside the SQL-only PostgreSQL extension runtime. It
claims rows from `fspg_ec_advisor.azure_monitor_outbox` and uploads normalized
advisory events through the Azure Monitor Logs Ingestion API.

Run it on an Azure-hosted workload with a Managed Identity. Do not use keys or
connection strings for Azure authentication.

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Configuration

| Environment variable | Required | Description |
| --- | --- | --- |
| `FSPG_EC_ADVISOR_DSN` | Yes | TLS-protected PostgreSQL DSN supplied through Key Vault or workload identity configuration. |
| `FSPG_EC_ADVISOR_DCR_ENDPOINT` | Yes | HTTPS Data Collection Endpoint or DCR ingestion endpoint. |
| `FSPG_EC_ADVISOR_DCR_IMMUTABLE_ID` | Yes | Azure Monitor Data Collection Rule immutable ID. |
| `FSPG_EC_ADVISOR_DCR_STREAM_NAME` | Yes | Custom DCR stream, for example `Custom-FspgEcAdvisorEvents_CL`. |
| `FSPG_EC_ADVISOR_MANAGED_IDENTITY_CLIENT_ID` | No | User-assigned Managed Identity client ID. Omit for system-assigned identity. |
| `FSPG_EC_ADVISOR_BATCH_SIZE` | No | Claimed records per upload; defaults to `50`. |
| `FSPG_EC_ADVISOR_LEASE_SECONDS` | No | Database outbox lease; defaults to `300`. |
| `FSPG_EC_ADVISOR_MAX_ATTEMPTS` | No | Database retry limit; defaults to `8`. |
| `FSPG_EC_ADVISOR_POLL_SECONDS` | No | Idle polling interval; defaults to `15`. |

## Run

```bash
python3 fspg_ec_advisor_azure_monitor.py --once
python3 fspg_ec_advisor_azure_monitor.py
```

The worker uploads only the advisor event envelope. It does not send raw query
text. On Azure API failure it marks the leased records for database-managed
exponential retry; after the configured attempt limit the extension marks them
`failed` for operator review.

## References

- [Azure Monitor Logs Ingestion API overview](https://learn.microsoft.com/azure/azure-monitor/logs/logs-ingestion-api-overview)
- [Azure Monitor Ingestion client library for Python](https://learn.microsoft.com/python/api/overview/azure/monitor-ingestion-readme)