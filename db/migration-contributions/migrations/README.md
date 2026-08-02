# backtest migrations

Only files matching `^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` may live
here, and only they enter the central Flyway bundle.

This directory currently contains no SQL. Every `backtest.*` table already exists in
the applied, immutable central baseline `V1__initial_schema.sql`, so this repository
has nothing to contribute yet. Do not re-declare those tables here.

Before adding a file:

1. Change the canonical root `db/schema.dbml` in the same reviewed change.
2. Use a UTC `yyyyMMddHHmmss` version that is globally unique across all owners.
3. Touch only the `backtest` schema — the central ownership verifier rejects anything else.
4. Never edit a migration that has been applied; add a later one.
