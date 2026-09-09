"""Write load provenance to the warehouse's audit.load_log table.

That table was created back in Sprint 0 with the rule "every pipeline run leaves
a trace." This module is where that promise gets kept for real.

WHY audit logging is not optional in banking:
When a report shows a wrong number, the first question is always "where did this
data come from, and when did it land?" Without a load log you are guessing.
With one you can answer: this table was loaded by this pipeline, at this time,
with this many rows, and it succeeded. That is lineage — and BCBS 239 expects
banks to be able to produce it.

DESIGN NOTE: we log the START of a run (status='running') and then UPDATE it on
completion. Why not just log at the end? Because a job that CRASHES never
reaches the end — and a crashed run that left no trace is invisible. A stale
'running' row is itself the signal that something died.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

import psycopg2

from .settings import WarehouseSettings

logger = logging.getLogger("meridian_ingestion.audit")


@contextmanager
def _connection(settings: WarehouseSettings):
    conn = psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
    )
    try:
        yield conn
    finally:
        conn.close()


def start_load(settings: WarehouseSettings, pipeline: str, target_table: str) -> int:
    """Record the start of a load. Returns the load_id to close out later."""
    with _connection(settings) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.load_log (pipeline, target_table, status)
            VALUES (%s, %s, 'running')
            RETURNING load_id
            """,
            (pipeline, target_table),
        )
        load_id = cur.fetchone()[0]
    logger.info("audit: started load_id=%s pipeline=%s target=%s", load_id, pipeline, target_table)
    return load_id


def finish_load(
    settings: WarehouseSettings, load_id: int, rows_loaded: int, status: str = "success"
) -> None:
    """Close out a load record with its final status and row count."""
    if status not in ("success", "failed"):
        raise ValueError(f"invalid status {status!r}")
    with _connection(settings) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.load_log
               SET rows_loaded = %s, status = %s, finished_at = now()
             WHERE load_id = %s
            """,
            (rows_loaded, status, load_id),
        )
    logger.info("audit: finished load_id=%s status=%s rows=%s", load_id, status, rows_loaded)


@contextmanager
def audited_load(settings: WarehouseSettings, pipeline: str, target_table: str):
    """Context manager that logs start, then success or failure automatically.

    Usage:
        with audited_load(settings, "ingest_raw", "customers") as record:
            ...do the work...
            record(rows_loaded=1234)

    If the body raises, the load is marked 'failed' and the exception still
    propagates — failures must be both recorded AND visible, never swallowed.
    """
    load_id = start_load(settings, pipeline, target_table)
    counter = {"rows": 0}

    def record(rows_loaded: int) -> None:
        counter["rows"] = rows_loaded

    try:
        yield record
    except Exception:
        finish_load(settings, load_id, counter["rows"], status="failed")
        raise
    else:
        finish_load(settings, load_id, counter["rows"], status="success")
