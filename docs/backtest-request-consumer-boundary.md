# Backtest request consumer boundary

`strategy-bot.v1` Basic requests and `backtest-request.v1` Custom/Competition
requests are carried by the backend transactional Outbox. The SQS
body is the Outbox JSONB rendering and these string message attributes are
required: `eventType`, `contractVersion`, `ownerDomain`, `aggregateId`,
`aggregateSequence`, `messageId`, `idempotencyKey`, `outboxIdempotencyKey`, and
`payloadHash`. `payloadHash` is the lowercase SHA-256 of the exact UTF-8 body
bytes. The consumer rejects a missing or mismatched attribute before invoking a
handler.

Basic, Custom and Competition use separate main queues and DLQs. A request delivered to
the other lane is permanent poison. The consumer receipt identity is
`(consumer_handler_id, messageId)`; the same ID and hash is a duplicate, the same
ID with another hash is a permanent conflict, and a lower sequence for the same
aggregate is acknowledged as stale without applying an effect.

## Producer fields implemented for relay enablement

The provider payloads carry the following immutable execution inputs. Relay still
requires the runtime activation checks at the end of this document.

### Custom

Keep the existing metadata and request fields, and add these required fields:

| JSON field | Type | Source/meaning |
| --- | --- | --- |
| `requestingAccountId` | UUID string | Authenticated account accepted by the backend owner check. It makes the existing producer-key material consumer-verifiable. |
| `expectedDatasetHash` | `sha256:<64 lowercase hex>` | Locked `market_data.dataset_manifests.dataset_hash`. |
| `instrumentCatalogVersion` | non-empty string | Immutable launch plan/catalog security-universe version. |
| `executionPolicyVersion` | non-empty string | Exact published execution policy; no nearest/current substitution. |
| `initialCashAmount` | canonical positive decimal string | Immutable launch plan initial cash. |

The producer key remains
`sha256("CUSTOM\n" + requestingAccountId + "\n" + clientIdempotencyKey)`.
The request hash must add the new semantic values in a documented fixed order.
The current `assumptionsVersion` is an accounting-rules value, not an execution
policy identity; rename it to `accountingRulesVersion`, or retain it only as a
compatibility field that must equal the selected policy's accounting version.

### Competition

Keep `roomId`, `participationId`, `botId`, `planVersion`, `planHash`, the bot
snapshot/compiled-plan hashes, and add:

| JSON field | Type |
| --- | --- |
| `scoringTemplateVersionId` | UUID string |
| `roomRulesHash` | `sha256:<64 lowercase hex>` |
| `initialCashAmount` | canonical positive decimal string |
| `currencyCode` | `USD` for the current product scope |
| `periods` | non-empty array in ascending `periodSequence` order |

Each period requires `evaluationPeriodId` (UUID), `periodSequence` (positive
integer), inclusive `evaluationStart`/`evaluationEnd` dates,
`importanceWeight` (canonical decimal string), `inputSetHash` (SHA-256), a
deterministically sorted `datasets` array, and a deterministically sorted
`featureMaterializations` array. Dataset entries require `datasetManifestId`,
`purposeCode`, and `expectedDatasetHash`; feature entries require
`featureMaterializationId` and `lockedResultHash`.

The Competition resolver must cross-check those values against:

- `competition.backtest_evaluation_plans` by `(room_id, plan_hash)`;
- `competition.backtest_evaluation_periods` by
  `(evaluation_plan_room_id, period_sequence)`;
- `competition.backtest_period_datasets` by
  `(evaluation_period_id, dataset_manifest_id, purpose_code)`;
- `competition.backtest_period_feature_materializations` by
  `(evaluation_period_id, feature_materialization_id)`;
- `competition.room_rules` for scoring, cash, currency, fee, buffer, precision,
  slippage and the locked room-rules hash;
- `bot.launch_snapshots` and `bot.launch_contract_plans` for the immutable bot
  release.

The provider resolves the exact execution-policy version and creates the
`competition.backtest_period_runs` linkage before publishing a Competition
request. The consumer cross-checks those provider-owned facts rather than
creating or repairing them.

## Runtime activation rule

The provider now pre-creates `backtest.runs` and `backtest.run_input_pins`.
Dispatch reads `run_input_pins.input_bundle_fingerprint`; it never substitutes
`runs.configuration_hash`, which remains the bot launch configuration hash.
Competition jobs preserve `evaluationPeriodId`, `inputSetHash`, all dataset pins
and all feature materialization pins. The worker re-resolves every dataset and
hash-checks it before execution, and publication persists the complete pin set.

Locked feature output consumption is fail-closed and identity-bound. For the active
Basic catalog, an RSI condition at `30m`, `1h`, `4h`, or `1d` requires the distinct
`RSI_14` definition and deterministic feature-output feed at that same resolution.
The runtime verifies the compiled feature UUID/resolution pair, materialization status,
result hash, output manifest and feed identity, object version, content hash, schema,
and exact prior/current bar positions before evaluation. Missing, stale, cross-resolution,
or gapped output is unavailable; RSI is never recomputed from closes in the backtest
worker. The legacy `RSI_14@1m` identity remains readable only for immutable historical
plans that already pin the legacy catalog.

`BacktestRequestIntake` and `PostgresRequestReceiptStore` are safe wire-boundary
components, not a substitute for the missing domain handler. The internal lane
worker consumes jobs containing `backtestRunId`; it must never be pointed at a
producer request queue. Enable the backend Custom/Competition relay routes only
after an end-to-end test proves request intake, immutable input resolution,
period linkage, run creation, job execution, result publication, duplicate,
reversed sequence, retry, and lane-specific DLQ behavior.
