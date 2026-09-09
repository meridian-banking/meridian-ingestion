"""Validate a dataset against its declared contract.

DESIGN: validation returns a REPORT, it does not raise on the first problem.
Why? Because "column 3 is wrong" is far less useful than "here are all 7 things
wrong with this file". Ops teams fix a file once, not seven times. Real
data-quality frameworks (Great Expectations, Soda) work the same way — collect
all violations, then decide what to do.

CHECK TYPES implemented here (these categories recur in Sprint 5's fuller
data-quality framework):
  - schema     : are the expected columns present?
  - nullability: are non-nullable columns actually populated?
  - type       : do values parse as the declared type?
  - domain     : are values within min/max, or in the allowed set?
  - pattern    : do IDs match their expected format?
  - uniqueness : is the declared primary key actually unique?
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

import pandas as pd


@dataclass
class Violation:
    """One specific thing wrong with the data."""

    check: str  # which check failed, e.g. "domain"
    column: str | None
    message: str
    failing_rows: int = 0


@dataclass
class ValidationReport:
    """The full outcome of validating one dataset against one contract."""

    entity: str
    contract_version: str
    row_count: int
    violations: list[Violation] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0

    def to_dict(self) -> dict:
        return {
            "entity": self.entity,
            "contract_version": self.contract_version,
            "row_count": self.row_count,
            "is_valid": self.is_valid,
            "violations": [
                {
                    "check": v.check,
                    "column": v.column,
                    "message": v.message,
                    "failing_rows": v.failing_rows,
                }
                for v in self.violations
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def load_contract(entity: str, contracts_dir: str | Path | None = None) -> dict:
    """Load an entity's contract JSON.

    Looks inside the installed package by default so contracts ship WITH the
    code (versioned together — a contract change is a code change, reviewable
    in a PR).
    """
    if contracts_dir is not None:
        path = Path(contracts_dir) / f"{entity}.json"
        if not path.exists():
            raise FileNotFoundError(f"No contract for entity {entity!r} at {path}")
        return json.loads(path.read_text())

    package = "meridian_ingestion.contracts"
    try:
        text = resources.files(package).joinpath(f"{entity}.json").read_text()
    except (FileNotFoundError, ModuleNotFoundError) as err:
        raise FileNotFoundError(f"No contract for entity {entity!r}") from err
    return json.loads(text)


def validate(df: pd.DataFrame, contract: dict) -> ValidationReport:
    """Check a DataFrame against a contract and return a full report."""
    report = ValidationReport(
        entity=contract["entity"],
        contract_version=contract["version"],
        row_count=len(df),
    )
    columns = contract["columns"]

    # --- schema: expected columns present? ---
    expected = {c["name"] for c in columns}
    actual = set(df.columns)
    missing = sorted(expected - actual)
    if missing:
        report.violations.append(Violation("schema", None, f"missing columns: {missing}"))
        # Can't run column checks on columns that aren't there.
        columns = [c for c in columns if c["name"] in actual]

    unexpected = sorted(actual - expected)
    if unexpected:
        # Extra columns are a WARNING-shaped problem, but we record them:
        # an unannounced new column often means the producer changed something.
        report.violations.append(Violation("schema", None, f"unexpected columns: {unexpected}"))

    for spec in columns:
        name = spec["name"]
        series = df[name]

        # --- nullability ---
        if not spec.get("nullable", True):
            n_null = int(series.isna().sum())
            if n_null:
                report.violations.append(
                    Violation("nullability", name, "nulls in non-nullable column", n_null)
                )

        non_null = series.dropna()
        if non_null.empty:
            continue

        # --- type ---
        bad_type = _type_violations(non_null, spec["type"])
        if bad_type:
            report.violations.append(
                Violation("type", name, f"values not parseable as {spec['type']}", bad_type)
            )

        # --- domain: min / max / allowed ---
        if "min" in spec or "max" in spec:
            numeric = pd.to_numeric(non_null, errors="coerce").dropna()
            if "min" in spec:
                n = int((numeric < spec["min"]).sum())
                if n:
                    report.violations.append(
                        Violation("domain", name, f"values below min {spec['min']}", n)
                    )
            if "max" in spec:
                n = int((numeric > spec["max"]).sum())
                if n:
                    report.violations.append(
                        Violation("domain", name, f"values above max {spec['max']}", n)
                    )

        if "allowed" in spec:
            allowed = set(spec["allowed"])
            n = int((~non_null.astype(str).isin(allowed)).sum())
            if n:
                report.violations.append(
                    Violation("domain", name, f"values outside allowed set {sorted(allowed)}", n)
                )

        # --- pattern ---
        if "pattern" in spec:
            rx = re.compile(spec["pattern"])
            n = int((~non_null.astype(str).str.match(rx)).sum())
            if n:
                report.violations.append(
                    Violation("pattern", name, f"values not matching {spec['pattern']}", n)
                )

    # --- uniqueness of the declared primary key ---
    pk = contract.get("primary_key", [])
    if pk and all(col in df.columns for col in pk):
        n_dupes = int(df.duplicated(subset=pk).sum())
        if n_dupes:
            report.violations.append(
                Violation("uniqueness", ",".join(pk), "duplicate primary keys", n_dupes)
            )

    return report


def _type_violations(series: pd.Series, declared: str) -> int:
    """Count values that don't parse as the declared type."""
    if declared in ("integer", "float"):
        coerced = pd.to_numeric(series, errors="coerce")
        bad = int(coerced.isna().sum())
        if declared == "integer" and bad == 0:
            # Reject non-whole numbers in an integer column.
            bad = int((coerced % 1 != 0).sum())
        return bad
    if declared in ("date", "timestamp"):
        coerced = pd.to_datetime(series, errors="coerce", format="mixed")
        return int(coerced.isna().sum())
    if declared == "boolean":
        ok = {True, False, "True", "False", "true", "false", 0, 1, "0", "1"}
        return int((~series.isin(ok)).sum())
    # strings: anything renders as a string
    return 0
