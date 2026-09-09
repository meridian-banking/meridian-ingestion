# ADR 0002: Store watermarks in the object store, not the warehouse

## Status
Accepted — 2026-07

## Context
Incremental loading needs durable state: "how far have I loaded entity X?".
Two obvious homes for that state:

1. The warehouse (a Postgres table), transactional with warehouse writes.
2. The object store itself, as small JSON objects.

## Decision
Store watermarks as JSON objects under a `_watermarks/` prefix in the raw bucket.

## Consequences
+ Ingestion has no hard dependency on the warehouse being reachable. Landing raw
  data can proceed even during a warehouse outage — which matters because raw
  landing is the step that must not be missed (the source may not retain the file).
+ No schema migration needed to add a new entity's watermark.
- Not transactional with the load: a crash between "data written" and "watermark
  updated" leaves the watermark behind, so the next run reprocesses that window.
  This is SAFE only because our writes are idempotent — reprocessing overwrites
  rather than duplicating. The correctness of this ADR depends on that property.
- Watermarks are not queryable via SQL alongside `audit.load_log`. Accepted:
  the CLI exposes `watermarks` for inspection.

## Revisit if
Loads ever become non-idempotent, or watermark state needs to participate in a
transaction with warehouse writes.
