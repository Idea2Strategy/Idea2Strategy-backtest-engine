# DBML change request: `backtest.run_input_pins`

*Status: **proposed**. Not approved, not integrated. The canonical `db/schema.dbml`
lives in the root superproject and is owned centrally; this repository cannot edit it
and does not claim to have.*

- **Requested by**: `backtest-engine`, card D29 (`docs/backend-implementation-master-checklist.md:338`)
- **Historical proposal fixture**: `db/migration-contributions/fixtures/superseded-proposals/V20260802094500__backtest_run_input_pins.sql.fixture`
- **Current provider-owned target fixture**: `db/migration-contributions/fixtures/pending-root/V20260805130000__backtest_run_input_pins.sql.fixture`
- **Schema touched**: `backtest` only (owner `backtest`, per `DatabaseAccessPolicy.SCHEMA_OWNERS`)
- **Date**: 2026-08-02

## Why

D29 requires the engine to serve 입력 데이터·모델 for one run. The reproducibility
boundary the API reports is eight values:

| value | canonical source today |
|---|---|
| `input_bundle_fingerprint` | `backtest.runs.configuration_hash` |
| `precision_rules_version` | `backtest.runs.precision_rules_version` |
| `dataset_manifest_id` | `backtest.input_datasets.dataset_manifest_id` (publish-time only) |
| `dataset_hash` | `backtest.input_datasets.locked_dataset_hash` (publish-time only) |
| `compiled_plan_checksum` | **none** |
| `strategy_snapshot_hash` | **none** |
| `feature_materialization_version` | **none** |
| `execution_policy_version` | **none** |

The last four are *inputs* of `runs.configuration_hash`
(`contracts.compute_input_bundle_fingerprint`), which is a SHA-256. A digest cannot be
inverted, so no join over the existing `backtest.*` tables can recover them. Before
this change the read model had exactly three options: invent them, report `null`, or
refuse to serve the route. All three fail D29.

The two publish-time values are additionally unavailable for a `QUEUED`, `FAILED` or
`UNAVAILABLE` run, which is precisely the case D29 calls out ("unavailable 이유"): a run
that never reached the worker has no `input_bundles` row and therefore no
`input_datasets` rows.

## Proposed DBML

```dbml
Table backtest.run_input_pins {
  run_id uuid [pk]
  compiled_plan_checksum varchar(128) [not null]
  strategy_snapshot_hash varchar(128) [not null]
  dataset_manifest_id uuid [not null]
  dataset_hash varchar(128) [not null]
  feature_materialization_version varchar(80) [not null]
  execution_policy_version varchar(80) [not null]
  pinned_at timestamptz [not null]

  Note: '공식 백테스트 요청이 고정한 입력 식별자. runs.configuration_hash 의 재료이며 단방향 해시에서 복원할 수 없다. 실행 수락 트랜잭션에서 1행 기록 후 불변.'
}

Ref: backtest.run_input_pins.run_id > backtest.runs.id
Ref: backtest.run_input_pins.dataset_manifest_id > market_data.dataset_manifests.id
```

## Semantics

- **Write-once, at acceptance.** The row is inserted in the same transaction as the
  `backtest.runs` insert (`lifecycle.PersistenceRunGateway.accept`). A run either has
  its pins or does not exist.
- **Idempotent.** Re-accepting the same request is `ON CONFLICT DO NOTHING`; the
  values are a pure function of the request, so a redelivery writes the same row. A
  *different* set of pins under a run id that already has them is rejected as a
  conflict, exactly as `runs.idempotency_key` rejects a divergent re-accept.
- **Never updated, never deleted independently of the run.**

## Deliberate redundancy with `backtest.input_datasets`

`dataset_manifest_id` / `dataset_hash` appear in both tables. `input_datasets` is the
*locked* bundle written by the worker; `run_input_pins` is what the request asked for.
They must agree, and the read model asserts they do
(`result_query.DurableBacktestResultQueryStore`) rather than preferring one silently.
If a reviewer prefers no redundancy, the alternative is to write the
`backtest.input_bundles` + `input_datasets` rows at acceptance instead of at publish;
that is a larger behavioural change to the publish path and is not proposed here.

## Rejected alternative: widen `backtest.runs`

Four more columns on `runs` would avoid a table. Rejected because `runs` is the
hot row every status transition updates, three of the four values are only ever read
by one endpoint, and the backend already `INSERT`s into `backtest.runs` directly
(`ImmutableStrategyReleaseJooqCommandAdapter.java:207`) — adding `NOT NULL` columns
there breaks that writer, and adding nullable ones re-creates the "report null"
problem this change exists to remove.

## Known gap this change does **not** close

`backtest.detail_manifests` has `supersedes_manifest_id` but no
`base_object_id` / `correction_of_object_id`, which
`detail_object_manifest.DetailObjectDescriptor` carries and folds into `detail_hash`.
A superseded detail part therefore cannot be reassembled on read; the read model
raises `QueryIntegrityError` naming the missing columns rather than reconstructing a
descriptor with the lineage silently dropped. Nothing in the engine publishes a
correction today (`DurableResultPublisher` never passes `supersedes=`), so this is a
future change request, not a live defect.
