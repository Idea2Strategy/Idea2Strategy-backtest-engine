# backtest migrations

Only files matching `^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` may live
here, and only they enter the central Flyway bundle.

Every table the applied, immutable central baseline `V1__initial_schema.sql` declares
is off limits here. Do not re-declare or alter one; add a later migration instead.

Contributed so far:

- `V20260802094500__backtest_run_input_pins.sql` — adds `backtest.run_input_pins`, the
  request identifiers `runs.configuration_hash` hashes over and cannot give back. See
  `../change-requests/2026-08-02-backtest-run-input-pins.md`.

Before adding a file:

1. Change the canonical root `db/schema.dbml` in the same reviewed change.
2. Use a UTC `yyyyMMddHHmmss` version that is globally unique across all owners.
3. Touch only the `backtest` schema — the central ownership verifier rejects anything else.
4. Never edit a migration that has been applied; add a later one.
