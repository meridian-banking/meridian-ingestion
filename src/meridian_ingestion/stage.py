"""Promote data from RAW to STAGED: apply types, convert to Parquet.

WHY A SEPARATE STAGING STEP?
Raw is what we received (possibly CSV, all strings, as-sent). Staged is what
analytics wants: typed columns, columnar storage, compressed. Keeping them
separate means we can rebuild staged from raw at any time — after a bug fix,
a contract change, or a type correction — without ever going back to the source
system.

WHY PARQUET (a guaranteed interview question):
  - COLUMNAR: a query reading 2 of 20 columns reads only those 2 files' worth of
    data. CSV forces you to read every byte of every row.
  - COMPRESSED: typically 5-10x smaller than the same data as CSV, because
    similar values sit adjacent in column order and compress extremely well.
  - TYPED: the schema travels WITH the data. A date is a date, not a string that
    every consumer must re-parse (and re-parse differently, causing bugs).
  - PREDICATE PUSHDOWN: row-group statistics let engines skip chunks that can't
    match a filter.
When is CSV still fine? Small files, human inspection, or handing data to a tool
that can't read Parquet. Otherwise, Parquet.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from .ingest import read_source_file
from .s3_client import download_to_file, list_keys, partitioned_key, upload_file
from .validation import load_contract

logger = logging.getLogger("meridian_ingestion.stage")


@dataclass
class StageResult:
    entity: str
    rows: int
    source_key: str
    target_key: str


def apply_contract_types(df: pd.DataFrame, contract: dict) -> pd.DataFrame:
    """Cast columns to the types declared in the contract.

    This is where 'the string 2024-03-15' becomes an actual date, and
    'the string 1234.56' becomes a float. Doing it once here means every
    downstream consumer inherits correct types instead of re-parsing.
    """
    out = df.copy()
    for spec in contract["columns"]:
        name = spec["name"]
        if name not in out.columns:
            continue
        declared = spec["type"]

        if declared == "integer":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int64")
        elif declared == "float":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("float64")
        elif declared == "date":
            out[name] = pd.to_datetime(out[name], errors="coerce", format="mixed").dt.date
        elif declared == "timestamp":
            out[name] = pd.to_datetime(out[name], errors="coerce", format="mixed")
        elif declared == "boolean":
            bool_map = {"true": True, "false": False, "1": True, "0": False}
            out[name] = out[name].astype(str).str.lower().map(bool_map)
        else:
            out[name] = out[name].astype("string")
    return out


def stage_entity(
    client,
    raw_bucket: str,
    staged_bucket: str,
    entity: str,
    load_date: date,
    contracts_dir: str | Path | None = None,
) -> list[StageResult]:
    """Read an entity's raw partition, type it, write Parquet to staged."""
    prefix = partitioned_key(entity, load_date, "")
    keys = [k for k in list_keys(client, raw_bucket, prefix) if not k.endswith(".json")]
    if not keys:
        logger.warning("stage: nothing in raw for %s at %s", entity, load_date)
        return []

    contract = load_contract(entity, contracts_dir)
    results: list[StageResult] = []

    with tempfile.TemporaryDirectory() as tmp:
        for key in keys:
            local = download_to_file(client, raw_bucket, key, Path(tmp) / Path(key).name)
            df = read_source_file(local)
            typed = apply_contract_types(df, contract)

            out_path = Path(tmp) / f"{entity}.parquet"
            typed.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

            target = partitioned_key(entity, load_date, f"{entity}.parquet")
            upload_file(client, staged_bucket, target, out_path)

            logger.info(
                "stage: %s %d rows -> s3://%s/%s", entity, len(typed), staged_bucket, target
            )
            results.append(StageResult(entity, len(typed), key, target))

    return results
