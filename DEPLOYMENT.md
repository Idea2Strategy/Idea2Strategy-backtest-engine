# Production runtime wiring

The API and worker have no in-memory production fallback. Configure the factory
targets below exactly; every referenced factory is implemented in
`backtest_engine.production`.

## API

```text
BACKTEST_AUTHENTICATOR=backtest_engine.production:api_authenticator
BACKTEST_OBJECT_STORE=backtest_engine.production:s3_object_store
BACKTEST_OWNER_DIRECTORY=backtest_engine.production:postgres_owner_directory
BACKTEST_COMPILED_PLAN_SOURCE=backtest_engine.production:postgres_compiled_plan_source
BACKTEST_DATASET_MANIFEST_SOURCE=backtest_engine.production:postgres_dataset_manifest_source
BACKTEST_EXECUTION_POLICY_CATALOG=backtest_engine.production:execution_policy_catalog
BACKTEST_DEAD_LETTER_SINK=backtest_engine.production:sqs_dead_letter_sink
```

The API additionally requires:

| Setting | Purpose |
| --- | --- |
| `BACKTEST_DATABASE_URL` | PostgreSQL SQLAlchemy URL; the runtime only verifies/uses migrations and never applies DDL. |
| `BACKTEST_QUEUE_URL` | BASIC lane SQS URL used by the API acceptance endpoint. |
| `BACKTEST_API_HOST`, `BACKTEST_API_PORT` | Listener address and port. |
| `BACKTEST_RESULTS_BUCKET` | Immutable result object bucket. |
| `BACKTEST_RESULTS_PREFIX` | Optional result key prefix; default `backtest-results`. |
| `BACKTEST_API_DLQ_URL` | SQS queue for permanently rejected API intake. |
| `BACKTEST_SESSION_HMAC_KEY_BASE64` | The same 32-byte-or-longer HMAC key used by backend opaque sessions, base64 encoded. Inject from a secret, never from source control. |
| `BACKTEST_RESULT_INGEST_TOKEN` | Dedicated worker-to-API bearer token. Inject from a secret and do not reuse a customer session. |
| `BACKTEST_RESULT_PRINCIPAL_ID` | Stable UUID identifying the internal result publisher in API evidence. |
| `BACKTEST_EXECUTION_POLICY_FILE` | Read-only JSON file containing `schemaVersion: 1` and at least one immutable policy. |

The authenticator checks the backend session digest, expiry, revocation, account
and login state, auth epoch, password credential version, and active sanctions in
PostgreSQL. The worker-only token receives only `backtest:results:write`.

## Worker

```text
BACKTEST_JOB_HANDLER=backtest_engine.production:orchestrator_job_handler
BACKTEST_EXECUTION_KEY_STORE=backtest_engine.production:postgres_execution_key_store
```

Lane mode requires all six `BACKTEST_{BASIC,CUSTOM,COMPETITION}_{QUEUE,DLQ}_URL`
settings. The defaults are BASIC 2, CUSTOM 1, COMPETITION 1 and total 4; they may
be stated explicitly with the corresponding `_MAX_CONCURRENCY` variables and
`BACKTEST_MAX_TOTAL_CONCURRENCY=4`.

The lane scheduler consumes internal execution jobs containing `backtestRunId`.
Do not point its CUSTOM or COMPETITION URL at backend-worker's
`backtest-request.v1` producer queues: those bodies are immutable request
envelopes and are rejected by the job worker. `BacktestRequestIntake` is the
fail-closed boundary for those producer queues. It verifies the Outbox message
attributes and raw payload hash, rejects the wrong lane, records the canonical
`operations.outbox_consumer_receipts` receipt, and prevents an older aggregate
sequence from applying after a newer one.

Enabling either producer route still requires a production request handler. In
particular, the current Custom payload does not expose the requesting account
needed to recompute its producer key, and the Competition payload identifies its
hidden periods and datasets only through `planHash`. Until the corresponding
owner-bound Custom resolver and Competition plan/period resolver are integrated,
leave `CUSTOM_BACKTEST_REQUESTED` and `COMPETITION_BACKTEST_REQUESTED` disabled in
the backend Outbox relay. Never substitute the three job queue URLs for those two
request queue URLs.

The receipt adapter factory is:

```text
BACKTEST_REQUEST_RECEIPT_STORE=backtest_engine.production:postgres_request_receipt_store
```

The database role must have row DML on
`operations.outbox_consumer_receipts` and read access to the referenced immutable
`operations.outbox_messages` rows; it must not receive write access to any other
`operations` table.

The worker also requires:

| Setting | Purpose |
| --- | --- |
| `BACKTEST_DATABASE_URL` | Shared PostgreSQL URL. |
| `BACKTEST_WORKER_ID` | Stable instance identity for attempt evidence. |
| `BACKTEST_RESULTS_BUCKET`, `BACKTEST_RESULTS_PREFIX` | Same result store as the API. |
| `BACKTEST_MARKET_DATA_BUCKET` | Immutable market-data input bucket. |
| `BACKTEST_MARKET_DATA_CACHE` | Writable private cache directory, normally `/tmp/idea2strategy-market-data`. |
| `BACKTEST_MARKET_DATA_BATCH_SIZE` | Optional bounded Arrow batch size; default 65,536 rows. |
| `BACKTEST_API_BASE_URL` | Internal API origin, without `/api/v1`. |
| `BACKTEST_RESULT_INGEST_TOKEN` | Same dedicated secret as the API. |
| `BACKTEST_EXECUTION_POLICY_FILE` | Same read-only policy document as the API. |
| `BACKTEST_RUNTIME_POLICY_FILE` | Read-only versioned attempt, microstructure, fractional eligibility, and risk-limit document. |
| `BACKTEST_WORKER_CORRELATION_ID` | Deployment-operation correlation identifier. |

AWS credentials are resolved by the SDK credential chain. On EC2, use the
instance profile; do not inject access keys. `AWS_REGION` selects the region and
`AWS_ENDPOINT_URL` is only for a local S3/SQS emulator.

## Required policy document shapes

`BACKTEST_EXECUTION_POLICY_FILE`:

```json
{
  "schemaVersion": 1,
  "policies": [{
    "version": "...",
    "releaseQuarter": "YYYY-QN",
    "periodStart": "...Z",
    "periodEnd": "...Z",
    "feeRate": "0.00000000",
    "slippageRateBps": 0,
    "timezone": "America/New_York",
    "sessionCalendar": "XNYS",
    "timestampUnit": "us",
    "priceArrowType": "double",
    "volumeArrowType": "int64",
    "marketDataSchemaVersion": "...",
    "calculationModelVersion": "...",
    "marketRulesVersion": "...",
    "accountingRulesVersion": "...",
    "precisionRulesVersion": "precision:1.0.0",
    "feePolicyId": "UUID",
    "buyingPowerBufferPolicyId": "UUID",
    "goodTillCancelledHorizonSeconds": 1,
    "maxOrderHorizonSeconds": 1
  }]
}
```

`BACKTEST_RUNTIME_POLICY_FILE`:

```json
{
  "schemaVersion": 1,
  "attempt": {
    "maxAttempts": 3,
    "leaseDurationSeconds": 300,
    "attemptTimeoutSeconds": 1800,
    "maxCpuTimeSeconds": 300,
    "maxMemoryBytes": 536870912
  },
  "microstructure": {
    "version": "...",
    "maxVolumeParticipationBps": 1000,
    "buyingPowerBufferPolicyId": "UUID",
    "buyingPowerBufferBps": 1
  },
  "fractional": {"policyVersion": "...", "instrumentIds": []},
  "riskLimits": {
    "maxStrategyNotional": "...",
    "maxGrossExposure": "...",
    "maxInstrumentExposure": "..."
  }
}
```

These examples describe shape, not approved values. Deployment must mount the
reviewed, checksum-pinned documents. Missing or malformed policy data stops the
process before it receives work.

## Remaining schema gate

The current `backtest.run_attempts` table has no opaque claim token,
`claim_expires_at`, heartbeat timestamp, cancellation marker, or attempt lineage
needed for fenced lease reclaim and cancellation races. The production adapter
therefore supplies durable duplicate execution exclusion with the existing unique
execution key, but cannot claim the stronger lease/cancellation guarantees without
a reviewed central Flyway/DBML change. Do not describe that portion as release-ready
until the migration and the corresponding worker tests are integrated.
