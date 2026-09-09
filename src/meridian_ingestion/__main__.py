"""Command-line interface for the ingestion service.

    # land generator output into the raw zone
    python -m meridian_ingestion ingest --source ./output/parquet --date 2024-03-15

    # promote raw -> staged (typed Parquet)
    python -m meridian_ingestion stage --entity customers --date 2024-03-15

    # do both for every contracted entity
    python -m meridian_ingestion run --source ./output/parquet --date 2024-03-15

Why a CLI rather than a notebook: Airflow (Sprint 6) needs something it can
invoke as a command with arguments. Anything that must run on a schedule has to
be callable without a human clicking cells.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime

from .audit import audited_load
from .ingest import ingest_directory
from .s3_client import build_client
from .settings import s3_settings_from_env, warehouse_settings_from_env
from .stage import stage_entity
from .validation import load_contract
from .watermark import advance_watermark, get_watermark

logger = logging.getLogger("meridian_ingestion")

CONTRACTED_ENTITIES = ["branches", "customers", "accounts", "transactions", "loans"]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-7s  %(name)-28s  %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_ingest(args) -> int:
    s3_cfg = s3_settings_from_env()
    client = build_client(s3_cfg)
    results = ingest_directory(
        client, s3_cfg.raw_bucket, args.source, args.date, entities=args.entities
    )

    accepted = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]

    for r in results:
        logger.info(r.summary())
    logger.info("ingest complete: %d accepted, %d rejected", len(accepted), len(rejected))

    if accepted and not args.no_watermark:
        for r in accepted:
            advance_watermark(client, s3_cfg.raw_bucket, r.entity, args.date)

    # A rejected file is a real failure signal — exit non-zero so Airflow's task
    # fails visibly rather than appearing to succeed with missing data.
    return 1 if rejected else 0


def cmd_stage(args) -> int:
    s3_cfg = s3_settings_from_env()
    client = build_client(s3_cfg)
    entities = args.entities or CONTRACTED_ENTITIES

    total = 0
    for entity in entities:
        try:
            load_contract(entity)
        except FileNotFoundError:
            logger.warning("no contract for %s, skipping", entity)
            continue
        results = stage_entity(client, s3_cfg.raw_bucket, s3_cfg.staged_bucket, entity, args.date)
        total += sum(r.rows for r in results)

    logger.info("stage complete: %d rows written to staged", total)
    return 0


def cmd_run(args) -> int:
    """Ingest then stage, wrapped in an audit-log record."""
    wh_cfg = warehouse_settings_from_env()
    try:
        with audited_load(wh_cfg, "meridian_ingestion", "raw+staged") as record:
            rc = cmd_ingest(args)
            if rc == 0:
                cmd_stage(args)
            record(rows_loaded=0)  # per-entity counts live in the logs
            if rc != 0:
                raise RuntimeError("one or more files were rejected")
    except Exception as err:  # noqa: BLE001 - we want the message, then exit
        logger.error("run failed: %s", err)
        return 1
    return 0


def cmd_watermarks(args) -> int:
    s3_cfg = s3_settings_from_env()
    client = build_client(s3_cfg)
    for entity in CONTRACTED_ENTITIES:
        wm = get_watermark(client, s3_cfg.raw_bucket, entity)
        logger.info("%-16s %s", entity, wm.isoformat() if wm else "(never loaded)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="meridian_ingestion")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="land source files into the raw zone")
    p_ing.add_argument("--source", required=True, help="generator output directory")
    p_ing.add_argument("--date", required=True, type=_parse_date, help="YYYY-MM-DD")
    p_ing.add_argument("--entities", nargs="*", default=None)
    p_ing.add_argument("--no-watermark", action="store_true")
    p_ing.set_defaults(func=cmd_ingest)

    p_stg = sub.add_parser("stage", help="promote raw -> staged Parquet")
    p_stg.add_argument("--date", required=True, type=_parse_date)
    p_stg.add_argument("--entities", nargs="*", default=None)
    p_stg.set_defaults(func=cmd_stage)

    p_run = sub.add_parser("run", help="ingest + stage, with audit logging")
    p_run.add_argument("--source", required=True)
    p_run.add_argument("--date", required=True, type=_parse_date)
    p_run.add_argument("--entities", nargs="*", default=None)
    p_run.add_argument("--no-watermark", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_wm = sub.add_parser("watermarks", help="show current watermarks")
    p_wm.set_defaults(func=cmd_watermarks)

    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
