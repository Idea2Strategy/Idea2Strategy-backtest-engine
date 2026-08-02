-- backtest: record the outcome detail the backtest.v1 result contract already carries.
--
-- Why this migration exists
-- -------------------------
-- schemas/backtest/v1/backtest-result.schema.json requires three fields that the
-- applied baseline has nowhere to put, so the server validated them and then threw
-- them away:
--
--   * UNAVAILABLE requires BOTH reasonCode AND missingRequirements (minItems 1).
--     Only reasonCode had a column (failure_code). "REQUIRED_DATA_MISSING" without
--     the list of requirements does not tell an operator which dataset is missing.
--   * COMPLETED requires resultManifestId. Without it a completed run cannot be
--     linked to the manifest holding its result, which is why the UI's completed
--     view had nothing to link to.
--   * FAILED requires retryable. Without it a failed run cannot say whether
--     re-queuing it could ever succeed.
--
-- All three are nullable: each belongs to exactly one terminal status, and a run
-- that is not in that status must say "not applicable" rather than carry a default
-- that reads as a decision. In particular retryable is NOT defaulted to false --
-- "we have not failed" and "we failed and it is not retryable" are different facts.
--
-- Scope: backtest.runs only. This contribution declares schemas=backtest and claims
-- no other schema.

ALTER TABLE "backtest"."runs"
  ADD COLUMN "result_manifest_id" uuid,
  ADD COLUMN "retryable" boolean,
  ADD COLUMN "missing_requirements" jsonb;

-- The storage half of the contract's `minItems: 1`. A JSON array is used rather than
-- text[] because the applied baseline models every repeated structure as jsonb and
-- declares no array-typed column anywhere; introducing the first one here would make
-- this table the odd one out for no gain.
--
-- The constraint refuses an empty array specifically: an UNAVAILABLE run whose
-- requirement list is `[]` would render as "nothing was missing", which is exactly
-- the reading the contract's minItems forbids.
-- `jsonb_path_exists` rather than `NOT EXISTS (SELECT ... jsonb_array_elements ...)`:
-- PostgreSQL rejects a subquery inside a CHECK outright ("cannot use subquery in
-- check constraint"), while the SQL/JSON path operator is immutable and evaluates
-- inline. The predicate reads "there is no element whose type is not string".
ALTER TABLE "backtest"."runs"
  ADD CONSTRAINT "runs_missing_requirements_is_a_non_empty_string_array"
  CHECK (
    "missing_requirements" IS NULL
    OR (
      jsonb_typeof("missing_requirements") = 'array'
      AND jsonb_array_length("missing_requirements") > 0
      AND NOT jsonb_path_exists("missing_requirements", '$[*] ? (@.type() != "string")')
    )
  );

COMMENT ON COLUMN "backtest"."runs"."result_manifest_id" IS 'COMPLETED 결과 이벤트의 resultManifestId. 완료된 실행을 결과 매니페스트에 연결하는 유일한 키이며 그 외 상태에서는 NULL이다.';
COMMENT ON COLUMN "backtest"."runs"."retryable" IS 'FAILED 결과 이벤트의 retryable. 재시도가 성공할 수 있는 실패인지를 기록하며 실패하지 않은 실행에서는 NULL이다. false 기본값을 쓰지 않는 이유는 미실패와 재시도 불가가 서로 다른 사실이기 때문이다.';
COMMENT ON COLUMN "backtest"."runs"."missing_requirements" IS 'UNAVAILABLE 결과 이벤트의 missingRequirements. 계약이 minItems 1 로 요구하는 문자열 배열이며 워커가 보낸 순서를 그대로 보존한다. 그 외 상태에서는 NULL이다.';
