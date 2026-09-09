# ADR 0003: Express data contracts as JSON files, not Python code

## Status
Accepted — 2026-07

## Context
Each entity needs a declared schema: columns, types, nullability, value ranges,
allowed values, ID patterns, primary key. This could live as Python (pydantic
models, dataclasses) or as data (JSON/YAML files).

## Decision
Contracts are versioned JSON files shipped inside the package
(`src/meridian_ingestion/contracts/*.json`).

## Consequences
+ A contract is an agreement between the team PRODUCING data and the team
  CONSUMING it. Expressing it as language-neutral data means a producer writing
  Java or a stakeholder reading the repo can both understand it.
+ Contracts version alongside code and change through PRs — a schema change is
  reviewable, not silent.
+ CI can validate contracts independently of the application (see the
  `contracts-valid` job).
- Less expressive than code: cross-column rules ("open_date >= join_date") cannot
  be stated. Those checks live in the Sprint 5 data-quality framework instead.
- No compile-time type safety; validation logic must interpret the JSON.
