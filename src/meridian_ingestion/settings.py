"""Runtime settings, read from environment variables.

WHY environment variables and not a config file?
This service needs CREDENTIALS (object-store keys, database password). Secrets
must never live in committed files — that's the same rule that put your
passwords in `.env` back in Sprint 0. Environment variables are the standard
mechanism (the "config" factor of the 12-factor app methodology): the same
code artifact runs in dev, UAT, and prod, and only the environment differs.

WHY this matters for the AWS migration (Sprint 9):
Every setting here is endpoint/credential related. Moving from local MinIO to
real AWS S3 becomes a matter of changing these values — NOT rewriting code.
That was the bet we made in ADR 0001 (local-first development), and this module
is where the bet pays off.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Settings:
    """Connection details for the object store (MinIO locally, S3 in cloud)."""

    endpoint_url: str | None  # None => real AWS S3; a URL => MinIO or similar
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    # The three lake zones, created back in Sprint 0's docker-compose.
    raw_bucket: str = "meridian-raw"
    staged_bucket: str = "meridian-staged"
    curated_bucket: str = "meridian-curated"


@dataclass(frozen=True)
class WarehouseSettings:
    """Connection details for the Postgres warehouse (for audit logging)."""

    host: str
    port: int
    database: str
    user: str
    password: str


def s3_settings_from_env() -> S3Settings:
    """Build S3 settings from environment variables.

    Locally these come from your .env / docker-compose. In AWS, the endpoint
    is unset (so boto3 talks to real S3) and credentials come from an IAM role
    rather than static keys — which is why endpoint_url is optional.
    """
    return S3Settings(
        endpoint_url=os.getenv("MINIO_ENDPOINT"),  # unset in real AWS
        access_key=os.getenv("MINIO_ACCESS_KEY", ""),
        secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        region=os.getenv("AWS_REGION", "us-east-1"),
        raw_bucket=os.getenv("RAW_BUCKET", "meridian-raw"),
        staged_bucket=os.getenv("STAGED_BUCKET", "meridian-staged"),
        curated_bucket=os.getenv("CURATED_BUCKET", "meridian-curated"),
    )


def warehouse_settings_from_env() -> WarehouseSettings:
    """Build warehouse settings from environment variables."""
    return WarehouseSettings(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        database=os.getenv("WAREHOUSE_DB", "meridian"),
        user=os.getenv("WAREHOUSE_USER", "meridian"),
        password=os.getenv("WAREHOUSE_PASSWORD", ""),
    )
