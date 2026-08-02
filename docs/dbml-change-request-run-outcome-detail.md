# DBML change request — `backtest.runs` outcome detail

The canonical database model is the root superproject's `db/schema.dbml`. It is not a
file of this repository, so this repository cannot make the change; this document is
the exact text the change must apply, so the migration and the model land together.

**Migration this pairs with:**
`db/migration-contributions/migrations/V20260802143000__backtest_run_outcome_detail.sql`

## Why

`backtest-engine/src/backtest_engine/schemas/backtest/v1/backtest-result.schema.json`
declares three fields **required** in their terminal branch that `backtest.runs` has
no column for. The server validated each one and then dropped it:

| status | required field | previously persisted as |
|---|---|---|
| `COMPLETED` | `resultManifestId` | *nothing* |
| `FAILED` | `retryable` | *nothing* |
| `UNAVAILABLE` | `reasonCode` **and** `missingRequirements` (`minItems: 1`) | only `reasonCode`, as `failure_code` |

Consequences that were observed against the live server during the D31 UI work: a
completed run could not be linked to its result manifest at all, and an UNAVAILABLE
run reported `REQUIRED_DATA_MISSING` without naming a single missing requirement.

## The change

Inside `Table backtest.runs`, after the existing `result_hash` column:

```
  result_manifest_id uuid [note: 'COMPLETED 결과 이벤트의 resultManifestId. 완료된 실행을 결과 매니페스트에 연결하는 유일한 키이며 그 외 상태에서는 NULL이다.']
  retryable boolean [note: 'FAILED 결과 이벤트의 retryable. 재시도가 성공할 수 있는 실패인지를 기록하며 실패하지 않은 실행에서는 NULL이다. false 기본값을 쓰지 않는 이유는 미실패와 재시도 불가가 서로 다른 사실이기 때문이다.']
  missing_requirements jsonb [note: 'UNAVAILABLE 결과 이벤트의 missingRequirements. 계약이 minItems 1 로 요구하는 문자열 배열이며 워커가 보낸 순서를 그대로 보존한다. 그 외 상태에서는 NULL이다.']
```

## Decisions worth reviewing rather than assuming

**All three are nullable, and none is defaulted.** Each belongs to exactly one
terminal status. `retryable` in particular is not defaulted to `false`: "this run has
not failed" and "this run failed and re-queuing cannot help" are different facts, and
a default would erase the difference for every run that never failed.

**`missing_requirements` is `jsonb`, not `text[]`.** The applied baseline declares no
array-typed column anywhere and models every repeated structure as `jsonb`. Adding
the schema's first array type for one column would make this table the exception for
no gain. `jsonb` arrays preserve order, which matters: the list is stored in the order
the worker sent it rather than re-sorted.

**A CHECK enforces the contract's `minItems: 1` in the database**, not only in the
application:

```sql
CHECK (
  "missing_requirements" IS NULL
  OR (
    jsonb_typeof("missing_requirements") = 'array'
    AND jsonb_array_length("missing_requirements") > 0
    AND NOT jsonb_path_exists("missing_requirements", '$[*] ? (@.type() != "string")')
  )
)
```

An UNAVAILABLE run whose requirement list is `[]` would render as "nothing was
missing", which is the reading the contract's `minItems` exists to forbid. DBML cannot
express this constraint, so it lives in the migration only; that asymmetry is
intentional and is noted here so a later DBML-to-SQL regeneration does not drop it.
