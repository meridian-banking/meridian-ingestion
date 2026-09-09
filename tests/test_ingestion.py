"""Tests for the ingestion service.

We use `moto` to simulate S3 in memory. That means tests run fast, need no
MinIO container and no AWS account, and still exercise the real boto3 code
paths — the same calls that will run against real S3 in Sprint 9.
"""

from __future__ import annotations

import json
from datetime import date

import boto3
import pandas as pd
import pytest
from meridian_ingestion.ingest import ingest_file
from meridian_ingestion.s3_client import list_keys, partitioned_key
from meridian_ingestion.stage import apply_contract_types, stage_entity
from meridian_ingestion.validation import load_contract, validate
from meridian_ingestion.watermark import advance_watermark, get_watermark, set_watermark
from moto import mock_aws

RAW = "meridian-raw"
STAGED = "meridian-staged"


@pytest.fixture
def good_customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": ["CUST_00000001", "CUST_00000002"],
            "first_name": ["Ada", "Grace"],
            "last_name": ["Lovelace", "Hopper"],
            "age": [36, 45],
            "annual_income": [80000.0, 95000.0],
            "credit_score": [720, 780],
            "dti": [0.25, 0.18],
            "segment": ["mass", "affluent"],
            "join_date": ["2020-01-15", "2019-06-30"],
            "home_branch_id": ["BR_00000001", "BR_00000002"],
        }
    )


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=RAW)
        client.create_bucket(Bucket=STAGED)
        yield client


# --- key construction ------------------------------------------------------


def test_partitioned_key_format():
    key = partitioned_key("transactions", date(2024, 3, 5), "transactions.csv")
    assert key == "transactions/year=2024/month=03/day=05/transactions.csv"


def test_partitioned_key_is_deterministic():
    a = partitioned_key("customers", date(2024, 1, 1), "customers.csv")
    b = partitioned_key("customers", date(2024, 1, 1), "customers.csv")
    assert a == b  # determinism is what makes writes idempotent


# --- validation ------------------------------------------------------------


def test_valid_data_passes(good_customers):
    report = validate(good_customers, load_contract("customers"))
    assert report.is_valid, report.to_dict()


@pytest.mark.parametrize(
    "column,value,expected_check",
    [
        ("credit_score", 9999, "domain"),  # above max
        ("credit_score", 100, "domain"),  # below min
        ("segment", "platinum", "domain"),  # not in allowed set
        ("customer_id", "NOPE_1", "pattern"),  # wrong ID format
        ("dti", 5.0, "domain"),  # above max
    ],
)
def test_invalid_values_are_caught(good_customers, column, value, expected_check):
    df = good_customers.copy()
    df.loc[0, column] = value
    report = validate(df, load_contract("customers"))
    assert not report.is_valid
    assert any(v.check == expected_check and v.column == column for v in report.violations)


def test_nulls_in_required_column_caught(good_customers):
    df = good_customers.copy()
    df.loc[0, "age"] = None
    report = validate(df, load_contract("customers"))
    assert any(v.check == "nullability" for v in report.violations)


def test_duplicate_primary_key_caught(good_customers):
    df = pd.concat([good_customers, good_customers.iloc[[0]]], ignore_index=True)
    report = validate(df, load_contract("customers"))
    assert any(v.check == "uniqueness" for v in report.violations)


def test_missing_column_caught(good_customers):
    df = good_customers.drop(columns=["segment"])
    report = validate(df, load_contract("customers"))
    assert any(v.check == "schema" for v in report.violations)


# --- ingestion -------------------------------------------------------------


def test_valid_file_lands_in_raw(s3, good_customers, tmp_path):
    path = tmp_path / "customers.csv"
    good_customers.to_csv(path, index=False)

    result = ingest_file(s3, RAW, "customers", path, date(2024, 3, 15))

    assert result.accepted
    assert result.key == "customers/year=2024/month=03/day=15/customers.csv"
    assert list_keys(s3, RAW) == [result.key]


def test_ingest_is_idempotent(s3, good_customers, tmp_path):
    """Re-running the same load must not create duplicate objects."""
    path = tmp_path / "customers.csv"
    good_customers.to_csv(path, index=False)

    for _ in range(3):
        ingest_file(s3, RAW, "customers", path, date(2024, 3, 15))

    assert len(list_keys(s3, RAW)) == 1


def test_invalid_file_is_dead_lettered_with_manifest(s3, good_customers, tmp_path):
    bad = good_customers.copy()
    bad.loc[0, "credit_score"] = 9999
    path = tmp_path / "customers.csv"
    bad.to_csv(path, index=False)

    result = ingest_file(s3, RAW, "customers", path, date(2024, 3, 15))

    assert not result.accepted
    assert result.key.startswith("_dead_letter/")

    keys = list_keys(s3, RAW, "_dead_letter")
    assert result.key in keys
    manifest_key = f"{result.key}.errors.json"
    assert manifest_key in keys

    manifest = json.loads(s3.get_object(Bucket=RAW, Key=manifest_key)["Body"].read())
    assert manifest["is_valid"] is False
    assert manifest["violations"]


def test_rejected_file_does_not_land_in_main_prefix(s3, good_customers, tmp_path):
    """Bad data must never appear where consumers would read it."""
    bad = good_customers.copy()
    bad.loc[0, "segment"] = "platinum"
    path = tmp_path / "customers.csv"
    bad.to_csv(path, index=False)

    ingest_file(s3, RAW, "customers", path, date(2024, 3, 15))

    clean_keys = [k for k in list_keys(s3, RAW) if not k.startswith("_dead_letter")]
    assert clean_keys == []


# --- watermarks ------------------------------------------------------------


def test_watermark_absent_initially(s3):
    assert get_watermark(s3, RAW, "customers") is None


def test_watermark_roundtrip(s3):
    set_watermark(s3, RAW, "customers", date(2024, 3, 15))
    assert get_watermark(s3, RAW, "customers") == date(2024, 3, 15)


def test_watermark_only_moves_forward(s3):
    set_watermark(s3, RAW, "customers", date(2024, 3, 15))
    advance_watermark(s3, RAW, "customers", date(2024, 3, 10))  # older
    assert get_watermark(s3, RAW, "customers") == date(2024, 3, 15)

    advance_watermark(s3, RAW, "customers", date(2024, 3, 20))  # newer
    assert get_watermark(s3, RAW, "customers") == date(2024, 3, 20)


# --- staging ---------------------------------------------------------------


def test_apply_contract_types_casts_correctly(good_customers):
    as_strings = good_customers.astype(str)
    typed = apply_contract_types(as_strings, load_contract("customers"))

    assert str(typed["credit_score"].dtype) == "Int64"
    assert str(typed["annual_income"].dtype) == "float64"
    assert isinstance(typed["join_date"].iloc[0], date)


def test_stage_writes_typed_parquet(s3, good_customers, tmp_path):
    path = tmp_path / "customers.csv"
    good_customers.to_csv(path, index=False)
    ingest_file(s3, RAW, "customers", path, date(2024, 3, 15))

    results = stage_entity(s3, RAW, STAGED, "customers", date(2024, 3, 15))

    assert len(results) == 1
    assert results[0].rows == 2
    assert results[0].target_key == "customers/year=2024/month=03/day=15/customers.parquet"
    assert list_keys(s3, STAGED) == [results[0].target_key]
