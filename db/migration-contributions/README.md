# Backtest migration contributions

This directory is the machine-readable handoff boundary from `backtest-engine` to the central Flyway bundle in `backend/db-migration`.

The launch schema was rebased on 2026-08-13. All schema and seed changes through that point are contained in the immutable central `V1__initial_schema.sql`, so `migrations/` intentionally has no timestamped migration yet. The vendored central fixture contains that same V1 for standalone tests.

Flyway remains the forward migration mechanism. After development starts, add reviewed changes as `V<UTC timestamp>__backtest_<slug>.sql`. This repository contributes SQL but never runs Flyway at application startup; `runtime.flyway.enabled` must remain `false`.

The root `db/schema.dbml` remains the canonical model. A new migration and its DBML change must be reviewed together.
