# backtest migrations

Only files matching `^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` may live
here, and only they enter the central Flyway bundle.

Every `backtest.*` **table** already exists in the applied, immutable central baseline
`V1__initial_schema.sql`. Do not re-declare those tables here; contribute only the
changes this repository legitimately needs on top of them.

## Contents

| file | change |
|---|---|
| `V20260802143000__backtest_run_outcome_detail.sql` | adds `result_manifest_id`, `retryable` and `missing_requirements` to `backtest.runs`, plus the CHECK that mirrors the `backtest.v1` UNAVAILABLE branch's `minItems: 1` |

`tests/conftest.py` applies every file here to the Testcontainers PostgreSQL 16 after
the vendored central bundle, and `tests/persistence/test_table_metadata.py` folds the
`ADD COLUMN` clauses into the expected schema, so a contributed column is verified
against a real database rather than only asserted in Python.

Before adding a file:

1. Change the canonical root `db/schema.dbml` in the same reviewed change.
2. Use a UTC `yyyyMMddHHmmss` version that is globally unique across all owners.
3. Touch only the `backtest` schema — the central ownership verifier rejects anything else.
4. Never edit a migration that has been applied; add a later one.
