"""Watermark store — remembers how far each entity has been loaded.

THE PROBLEM IT SOLVES:
You have three years of history. Reloading all of it every night is wasteful and
slow. Instead you track "the last date I successfully loaded for this entity"
— the WATERMARK — and each run processes only what's newer.

This is the simplest form of incremental loading. Production systems often use
proper Change Data Capture (CDC) tools like Debezium that stream row-level
changes out of the source database's transaction log. Watermarks are the
low-tech cousin: coarser (date-level, not row-level) but dependency-free, and
perfectly adequate when the source is a daily batch file.

WHERE WE STORE IT (a real design trade-off worth being able to defend):
We keep watermarks as small JSON objects in the object store itself, under a
`_watermarks/` prefix. Pros: no extra infrastructure, and ingestion stays
independent of the warehouse being up. Cons: not transactional with the load
itself — a crash between "data written" and "watermark updated" leaves the
watermark behind, so the next run reprocesses. That is SAFE precisely because
our loads are idempotent: reprocessing overwrites rather than duplicating.
Idempotency is what makes this simple design correct.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from botocore.exceptions import ClientError

logger = logging.getLogger("meridian_ingestion.watermark")

_PREFIX = "_watermarks"


def _key(entity: str) -> str:
    return f"{_PREFIX}/{entity}.json"


def get_watermark(client, bucket: str, entity: str) -> date | None:
    """Return the last successfully loaded date for an entity, or None."""
    try:
        obj = client.get_object(Bucket=bucket, Key=_key(entity))
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    payload = json.loads(obj["Body"].read())
    return date.fromisoformat(payload["watermark"])


def set_watermark(client, bucket: str, entity: str, value: date) -> None:
    """Record the last successfully loaded date for an entity."""
    body = json.dumps({"entity": entity, "watermark": value.isoformat()}).encode()
    client.put_object(Bucket=bucket, Key=_key(entity), Body=body)
    logger.info("watermark: %s -> %s", entity, value.isoformat())


def advance_watermark(client, bucket: str, entity: str, candidate: date) -> date:
    """Move the watermark forward only (never backwards).

    Guarding against regression matters: a late-arriving or manually re-run
    older batch must not rewind the marker and cause everything after it to be
    reprocessed forever.
    """
    current = get_watermark(client, bucket, entity)
    new_value = candidate if current is None or candidate > current else current
    if current != new_value:
        set_watermark(client, bucket, entity, new_value)
    return new_value
