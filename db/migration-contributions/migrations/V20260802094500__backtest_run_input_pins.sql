-- Card D29: the pinned request inputs a completed run must be able to report back.
--
-- `GET /api/v1/backtests/{run_id}/inputs` answers D29's "입력 데이터·모델" with the
-- eight values that make one run reproducible. Five of them already exist in the
-- applied baseline (`runs.configuration_hash`, `runs.precision_rules_version`, and
-- `input_datasets.dataset_manifest_id` / `locked_dataset_hash` once the run
-- publishes). Four do not exist anywhere in `backtest.*`:
--
--   * `compiled_plan_checksum`        - B's `basic-compiled-plan` planChecksum
--   * `strategy_snapshot_hash`        - B's `expectedSnapshotHash`
--   * `feature_materialization_version`
--   * `execution_policy_version`
--
-- They are inputs of `runs.configuration_hash`, which is a one-way digest, so they
-- cannot be recovered from it. Without this table the read model has to invent them
-- or report null, and the API cannot state the reproducibility boundary it exists to
-- state.
--
-- The row is written once, in the same transaction as the `backtest.runs` insert, so
-- it is available at every run status including QUEUED, FAILED and UNAVAILABLE. It is
-- never updated: `runs.idempotency_key` already makes re-acceptance idempotent, and a
-- second acceptance of the same request writes the identical values.
--
-- `dataset_manifest_id` and `dataset_hash` are repeated here deliberately.
-- `backtest.input_datasets` carries the same pair, but only from the moment the worker
-- locks the input bundle; a QUEUED or UNAVAILABLE run has no bundle at all. The read
-- model cross-checks the two whenever both exist and fails closed when they disagree,
-- so the repetition is a consistency check rather than a second source of truth.
--
-- Owner: backtest. Touches only the `backtest` schema.
-- Canonical model change request: db/migration-contributions/change-requests/
--   2026-08-02-backtest-run-input-pins.md (root `db/schema.dbml` is not in this
--   repository; it lives in the superproject and is owned centrally).

CREATE TABLE "backtest"."run_input_pins" (
  "run_id" uuid PRIMARY KEY,
  "input_bundle_hash" varchar(128) NOT NULL,
  "compiled_plan_checksum" varchar(128) NOT NULL,
  "strategy_snapshot_hash" varchar(128) NOT NULL,
  "dataset_manifest_id" uuid NOT NULL,
  "dataset_hash" varchar(128) NOT NULL,
  "feature_materialization_version" varchar(80) NOT NULL,
  "execution_policy_version" varchar(80) NOT NULL,
  "pinned_at" timestamptz NOT NULL
);

ALTER TABLE "backtest"."run_input_pins" ADD FOREIGN KEY ("run_id") REFERENCES "backtest"."runs" ("id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "backtest"."run_input_pins" ADD FOREIGN KEY ("dataset_manifest_id") REFERENCES "market_data"."dataset_manifests" ("id") DEFERRABLE INITIALLY IMMEDIATE;

COMMENT ON TABLE "backtest"."run_input_pins" IS
  '공식 백테스트 요청이 고정한 입력 식별자. runs.configuration_hash 의 재료이며 단방향 해시에서 복원할 수 없다. 실행 수락 트랜잭션에서 1행 기록 후 불변.';
