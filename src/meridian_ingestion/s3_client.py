"""Object-store client — works identically against MinIO (local) and AWS S3.

THE S3 OBJECT MODEL (a common interview topic):
S3 has no real folders. It is a flat key-value store: a *bucket* holds *objects*,
each identified by a *key* which is just a string. When you see

    transactions/year=2024/month=03/day=15/transactions.csv

...those slashes are part of one long key, not a directory tree. Tools display
them as folders for convenience. This matters because "renaming a folder" or
"moving a directory" are not cheap metadata operations like on a real
filesystem — they mean copying every object.

PARTITIONING:
The `year=YYYY/month=MM/day=DD` style is Hive-style partitioning. Query engines
(Athena, Spark, Glue — Sprint 9) understand this convention and can skip whole
prefixes when a query filters on date. Skipping data you never read is the
single biggest lever on both query speed and cloud cost.

IDEMPOTENCY:
Keys are DETERMINISTIC — the same entity + same date always produces the same
key. Writing twice overwrites rather than duplicating. That is what makes a
retry safe, which is what makes automated orchestration (Sprint 6) possible.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .settings import S3Settings

logger = logging.getLogger("meridian_ingestion.s3")


def build_client(settings: S3Settings):
    """Create a boto3 S3 client.

    When endpoint_url is set we're talking to MinIO; when it's None, boto3
    resolves the real AWS S3 endpoint. Same client object either way — this is
    the whole reason local development transfers to cloud without a rewrite.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key or None,
        aws_secret_access_key=settings.secret_key or None,
        region_name=settings.region,
        # s3v4 signing + path-style addressing keeps MinIO happy.
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def partitioned_key(entity: str, load_date: date, filename: str) -> str:
    """Build a deterministic, Hive-partitioned object key.

    Example:
        partitioned_key("transactions", date(2024, 3, 15), "transactions.csv")
        -> "transactions/year=2024/month=03/day=15/transactions.csv"

    Deterministic = idempotent: re-running the same load writes the same key.
    """
    return (
        f"{entity}/"
        f"year={load_date.year:04d}/"
        f"month={load_date.month:02d}/"
        f"day={load_date.day:02d}/"
        f"{filename}"
    )


def upload_file(client, bucket: str, key: str, path: str | Path) -> int:
    """Upload a local file to bucket/key. Returns bytes uploaded.

    Overwrites by design (idempotent). S3 PUT is atomic per object: a reader
    sees either the old object or the new one, never a half-written file.
    """
    path = Path(path)
    size = path.stat().st_size
    client.upload_file(str(path), bucket, key)
    logger.info("uploaded s3://%s/%s (%d bytes)", bucket, key, size)
    return size


def upload_bytes(client, bucket: str, key: str, payload: bytes) -> int:
    """Upload raw bytes (used for error manifests and small artifacts)."""
    client.put_object(Bucket=bucket, Key=key, Body=payload)
    logger.info("uploaded s3://%s/%s (%d bytes)", bucket, key, len(payload))
    return len(payload)


def download_to_file(client, bucket: str, key: str, dest: str | Path) -> Path:
    """Download an object to a local path (used by the staging step)."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(dest))
    return dest


def list_keys(client, bucket: str, prefix: str = "") -> list[str]:
    """List every object key under a prefix, handling pagination.

    NOTE: S3 returns at most 1000 keys per call. Forgetting to paginate is a
    classic bug — your code silently works in dev (few files) and silently
    misses data in production. The paginator handles it for us.
    """
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def object_exists(client, bucket: str, key: str) -> bool:
    """Return True if the object exists (a cheap HEAD request, not a download)."""
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise
