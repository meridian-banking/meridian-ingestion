# meridian-ingestion

Ingestion service for the **Meridian Financial Intelligence & Risk Analytics Platform**. Lands source data into the lake's raw zone with contract validation, dead-letter quarantine, idempotent writes, and watermark-based incremental loading — then promotes it to the staged zone as typed Parquet.

Consumes output from `meridian-data-generator`. Feeds `meridian-warehouse`.

## The flow

```
generator output  ──▶  VALIDATE  ──▶  raw zone (partitioned, as-received)
                          │                    │
                       invalid                 ▼
                          │            apply types, convert
                          ▼                    │
                  _dead_letter/                ▼
                  + errors.json         staged zone (typed Parquet)
```

**Raw is immutable** — data lands exactly as received, never cleaned or corrected in place. Cleaning happens downstream, so the original evidence of what the source sent is always recoverable (a real regulatory expectation under BCBS 239).

## Quick start

The lake must be running — start it from `meridian-infra` with `make up`.

```bash
pip install -e ".[dev]"

# Point at the local MinIO lake (values from meridian-infra/.env)
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=meridian
export MINIO_SECRET_KEY=<your minio password>
export WAREHOUSE_HOST=localhost
export WAREHOUSE_PASSWORD=<your warehouse password>

# Land generator output into raw, then promote to staged
python -m meridian_ingestion run \
  --source ../meridian-data-generator/output/parquet \
  --date 2024-03-15

# Inspect incremental-load state
python -m meridian_ingestion watermarks
```

Commands: `ingest` (raw only), `stage` (raw → staged), `run` (both, audit-logged), `watermarks`.

## Key design decisions

**Partitioned keys.** Objects land at `entity/year=YYYY/month=MM/day=DD/file`. This Hive-style convention lets query engines (Athena, Glue, Spark — Sprint 9) skip whole prefixes when filtering on date. Skipping data you never read is the biggest lever on both query speed and cloud cost.

**Idempotency.** Keys are deterministic, so re-running a load overwrites rather than duplicating. This is what makes retries safe — and therefore what makes automated orchestration (Sprint 6) possible.

**Data contracts.** Each entity has a versioned JSON contract in `src/meridian_ingestion/contracts/` declaring columns, types, nullability, ranges, allowed values, ID patterns, and primary key. Contracts are JSON rather than Python because a contract is an agreement between producer and consumer, and should be language-neutral.

**Dead-lettering, never dropping.** A file failing validation is quarantined under `_dead_letter/` with a machine-readable `.errors.json` manifest listing every violation. Nothing is silently skipped and nothing bad is force-loaded.

**Watermarks.** Per-entity `_watermarks/<entity>.json` records the last successfully loaded date, and only ever moves forward. Coarser than true CDC (Debezium et al.), but dependency-free and correct given idempotent loads.

**Cloud portability.** `settings.py` reads everything from environment variables and `endpoint_url` is optional — set it for MinIO locally, leave it unset for real AWS S3. Same code, both targets. This is ADR 0001 (local-first development) paying off.

## Development

```bash
make install    # pip install -e ".[dev]"
make test       # pytest (uses moto — no MinIO or AWS needed)
make lint       # ruff check + format check (same as CI)
make fmt        # auto-fix
```

Tests use [moto](https://github.com/getmoto/moto) to simulate S3 in memory, exercising the real boto3 code paths without any running infrastructure.

## Architecture

```
src/meridian_ingestion/
├── settings.py     # env-based config (S3 + warehouse)
├── s3_client.py    # boto3 wrapper, partitioned keys, pagination
├── contracts/      # versioned JSON data contracts per entity
├── validation.py   # contract enforcement → ValidationReport
├── ingest.py       # raw landing + dead-lettering
├── stage.py        # raw → staged, type casting, Parquet
├── watermark.py    # incremental-load state
├── audit.py        # writes to warehouse audit.load_log
└── __main__.py     # CLI
```

Part of the 8-repository Meridian platform.
