# backtest migrations

Only files matching `^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` may live
here, and only they enter the central Flyway bundle.

Every table the applied, immutable central baseline `V1__initial_schema.sql` declares
is off limits here. Do not re-declare or alter one of those tables; contribute only
the changes this repository legitimately needs on top of them. A genuinely new
`backtest.*` table this repository owns — one the baseline does not declare — is a
legitimate contribution; re-declaring a baseline table is not.

## Contents

| file | change |
|---|---|
| `V20260805170000__backtest_run_outcome_detail.sql` | forward-only addition of `result_manifest_id`, `retryable` and `missing_requirements` to `backtest.runs`, plus the CHECK that mirrors the `backtest.v1` UNAVAILABLE branch's `minItems: 1` |

The pending provider-owned table fixture applies at `V20260805130000`, followed by
the active outcome columns at `V20260805170000`. The historical `V20260802143000`
proposal is retained only under `fixtures/superseded-proposals` and is never scanned
as production Flyway SQL.

The historical consumer-owned `V20260802094500` input-pin proposal is retained in the
same fixture directory. The active directory must not contain it: the root provider's
normalized `V20260805130000` already creates that table.

`tests/conftest.py` applies every file here to the Testcontainers PostgreSQL 16 after
the vendored central bundle, and `tests/persistence/test_table_metadata.py` holds both
shapes to the same standard — a contributed `CREATE TABLE` is parsed like a baseline
one, and contributed `ADD COLUMN` clauses are folded into the expected schema — so a
contributed table or column is verified against a real database rather than only
asserted in Python.

Before adding a file:

1. Change the canonical root `db/schema.dbml` in the same reviewed change.
2. Use a UTC `yyyyMMddHHmmss` version that is globally unique across all owners.
3. Touch only the `backtest` schema — the central ownership verifier rejects anything else.
4. Never edit a migration that has been applied; add a later one.
