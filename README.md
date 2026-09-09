# JsExpert Stream

`jsexpert-stream` is the multi-tenant ingestion boundary for JsExpert. It authenticates an SDK request against the project registry, derives the tenant and project identifiers server-side, and publishes durable events to Kafka.

## Event flow

```text
SDK / application -> Rust ingestion API -> Kafka -> Rust consumer -> ClickHouse
```

Topics are split by event type:

- `jsexpert.events.trace`
- `jsexpert.events.error`
- `jsexpert.events.activity`
- `jsexpert.events.dlq` for poison messages

Every Kafka record uses `tenant_id:project_id:event_type` as its key. This keeps a tenant-project-event stream ordered within a partition while allowing independent projects and event kinds to scale across partitions. The API never accepts tenant or project identity from a payload; it derives both after validating `clientId` and `clientSecret` against PostgreSQL.

## Guarantees

- Kafka producer uses idempotence, `acks=all`, retries, and compressed records.
- Consumers disable auto-commit and commit a Kafka offset only after ClickHouse persists the event.
- `event_id` is preserved end-to-end for idempotent storage and retry-safe processing.
- Events that cannot be decoded by a consumer are sent to the DLQ with the processing error.
- Error payloads receive a stable fingerprint; activity events retain event name and actor ID for product-usage analysis.

## HTTP API

Both endpoints require the existing SDK headers `clientId` and `clientSecret`.

`POST /v1/events` accepts an envelope such as:

```json
{
  "event_type": "activity",
  "event_name": "dashboard_opened",
  "actor_id": "user-123",
  "payload": { "page": "/dashboard" }
}
```

`POST /v1/traces` accepts an OTLP JSON export directly and publishes it as a trace event, so the existing OpenTelemetry exporter can use this service without reshaping its payload.

## Local development

Run from the workspace root:

```sh
docker compose up --build kafka kafka-init jsexpert-stream
```

The service listens on `http://localhost:4002`. Health probes are available at `/health/live` and `/health/ready`.
