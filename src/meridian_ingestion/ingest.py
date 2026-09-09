"""Land source files into the RAW zone, with validation and dead-lettering.

THE FLOW:
    local file -> read -> validate against contract
                            |
                  valid ---+--- invalid
                    |             |
              raw bucket    dead-letter prefix
           (partitioned)   (+ error manifest)

KEY RULES:
1. RAW IS IMMUTABLE IN SPIRIT: we write the file as received. We do not clean,
   fix, or reshape it here. Cleaning happens later (staging / Sprint 5). If we
   "helpfully" corrected data on the way in, we would destroy the evidence of
   what the source actually sent — and that evidence is exactly what auditors
   and debugging sessions need.

2. NOTHING IS SILENTLY DROPPED. A file that fails validation is not discarded
   and not force-loaded: it goes to the dead-letter prefix alongside a JSON
   error manifest explaining precisely what was wrong. Someone can then fix the
   source and replay it. "Silently skip the bad file" is the worst possible
   behaviour in a regulated environment.

3. WRITES ARE IDEMPOTENT. Keys are deterministic (entity + date), so re-running
   overwrites rather than duplicating. Retry-safe by construction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from .s3_client import partitioned_key, upload_bytes, upload_file
from .validation import ValidationReport, load_contract, validate

logger = logging.getLogger("meridian_ingestion.ingest")

DEAD_LETTER_PREFIX = "_dead_letter"


@dataclass
class IngestResult:
    """Outcome of ingesting one file."""

    entity: str
    accepted: bool
    rows: int
    key: str
    report: ValidationReport

    def summary(self) -> str:
        state = "ACCEPTED" if self.accepted else "REJECTED"
        return f"{state} {self.entity}: {self.rows:,} rows -> {self.key}"


def read_source_file(path: str | Path) -> pd.DataFrame:
    """Read a source file into a DataFrame based on its extension.

    We read with dtype=str for CSV deliberately: raw ingestion should not
    guess types. Type enforcement is the contract's job (validation), and real
    typing happens in the staging step. Letting pandas infer here would mask
    problems — e.g. silently turning a malformed number into NaN.
    """
    path = Path(path)
    if path.suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported source file type: {path.suffix}")


def ingest_file(
    client,
    raw_bucket: str,
    entity: str,
    path: str | Path,
    load_date: date,
    contracts_dir: str | Path | None = None,
) -> IngestResult:
    """Validate one file and land it in raw (or dead-letter it)."""
    path = Path(path)
    df = read_source_file(path)
    contract = load_contract(entity, contracts_dir)
    report = validate(df, contract)

    if report.is_valid:
        key = partitioned_key(entity, load_date, path.name)
        upload_file(client, raw_bucket, key, path)
        logger.info("ingest: accepted %s (%d rows) -> %s", entity, len(df), key)
        return IngestResult(entity, True, len(df), key, report)

    # --- rejected: dead-letter the file AND an explanatory manifest ---
    dl_key = f"{DEAD_LETTER_PREFIX}/{partitioned_key(entity, load_date, path.name)}"
    upload_file(client, raw_bucket, dl_key, path)
    upload_bytes(client, raw_bucket, f"{dl_key}.errors.json", report.to_json().encode())
    logger.warning(
        "ingest: REJECTED %s (%d violations) -> %s",
        entity,
        len(report.violations),
        dl_key,
    )
    return IngestResult(entity, False, len(df), dl_key, report)


def ingest_directory(
    client,
    raw_bucket: str,
    source_dir: str | Path,
    load_date: date,
    entities: list[str] | None = None,
    contracts_dir: str | Path | None = None,
) -> list[IngestResult]:
    """Ingest every entity found under a generator output directory.

    Expects the layout the Sprint 1 generator produces:
        <source_dir>/<entity>/<entity>.csv|parquet

    Entities without a contract are skipped with a warning rather than failing
    the whole run — one missing contract should not block the other nine loads.
    """
    source_dir = Path(source_dir)
    results: list[IngestResult] = []

    for entity_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
        entity = entity_dir.name
        if entities and entity not in entities:
            continue

        files = sorted(list(entity_dir.glob("*.csv")) + list(entity_dir.glob("*.parquet")))
        if not files:
            logger.warning("ingest: no data files for entity %s", entity)
            continue

        try:
            load_contract(entity, contracts_dir)
        except FileNotFoundError:
            logger.warning("ingest: no contract for %s, skipping", entity)
            continue

        for f in files:
            results.append(ingest_file(client, raw_bucket, entity, f, load_date, contracts_dir))

    return results
