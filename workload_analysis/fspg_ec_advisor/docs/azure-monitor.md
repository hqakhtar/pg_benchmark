# Azure Monitor Delivery

`fspg_ec_advisor` never stores Azure credentials and never sends HTTP from the
PostgreSQL server. It writes durable alert records to
`fspg_ec_advisor.azure_monitor_outbox`; an Azure-hosted dispatcher delivers them
to Azure Monitor.

## Recommended Flow

```text
PostgreSQL run_advisory_cycle()
    -> advisory_event
    -> azure_monitor_outbox
    -> Managed Identity dispatcher
    -> Azure Monitor Logs Ingestion API / DCR
    -> FspgEcAdvisorEvents_CL
    -> Scheduled query alert rule
    -> Azure Monitor action group
```

Use a managed identity for the dispatcher. Grant it only the data-plane role
required to write through the target Data Collection Rule, scoped to that DCR.
Keep PostgreSQL credentials in Key Vault or workload identity configuration; do
not store them in the extension or outbox payload.

## Dispatcher Contract

Poll or listen for outbox work. `NOTIFY fspg_ec_advisor_azure_monitor` is a
latency hint only; polling is required for reliability.

The dispatcher requires the Data Collection Rule immutable ID, its HTTPS
ingestion endpoint, a custom stream name, and a TLS-protected PostgreSQL DSN.

```sql
-- Lease up to 50 records for five minutes.
SELECT *
FROM fspg_ec_advisor.claim_azure_monitor_outbox(
  p_limit => 50,
  p_lease_seconds => 300
);

-- After a successful Logs Ingestion API request.
SELECT fspg_ec_advisor.complete_azure_monitor_delivery(
  p_outbox_id => <outbox_id>,
  p_success => true
);

-- On a transient delivery failure. The extension retries with exponential
-- backoff and eventually marks the record failed.
SELECT fspg_ec_advisor.complete_azure_monitor_delivery(
  p_outbox_id => <outbox_id>,
  p_success => false,
  p_error => 'HTTP 429 from Logs Ingestion API'
);
```

Each outbox `payload` uses this stable envelope:

```json
{
  "schemaVersion": "1.0",
  "eventType": "fspg_ec_advisor.advisory",
  "eventTime": "2026-09-21T00:00:00Z",
  "data": {
    "eventId": 42,
    "action": "SCALE_UP_CANDIDATE",
    "severity": "WARNING",
    "captureKey": "..."
  }
}
```

Map the `data` fields and raw JSON payload into a Log Analytics custom table,
for example `FspgEcAdvisorEvents_CL`. Create a scheduled query alert rule over
that table and attach an Azure Monitor action group for Teams, email, ITSM, or
webhook notification.

## Operations

- Run the dispatcher with retry, exponential backoff, idempotent delivery, and
  structured logs.
- Treat `failed` outbox rows as an operational alert for the dispatcher itself.
- Scope database access to execute only the outbox claim/complete functions and
  read the rows returned by `claim_azure_monitor_outbox`.
- Do not send query text unless it is explicitly approved for the target Log
  Analytics workspace; advisor events intentionally carry the capture summary.

## Test Boundary

The extension's PostgreSQL regression tests cover outbox activation, claiming,
success acknowledgement, retry, terminal failure, suppression, and resolution.
They do not call Azure. Validate the dispatcher with a non-production Data
Collection Rule, Log Analytics workspace, scheduled query alert rule, and action
group before enabling it for production delivery.

## References

- [Azure Monitor Logs Ingestion API overview](https://learn.microsoft.com/azure/azure-monitor/logs/logs-ingestion-api-overview)
- [Azure Monitor Ingestion client library for Python](https://learn.microsoft.com/python/api/overview/azure/monitor-ingestion-readme)
