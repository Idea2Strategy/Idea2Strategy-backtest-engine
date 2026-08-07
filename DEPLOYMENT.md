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
| `CUSTOMER_JWT_SIGNING_KEY_BASE64` | The same 32-byte-or-longer HMAC key the backend uses to sign customer access JWTs, base64 encoded. Inject from a secret, never from source control. |
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

The current backend provider creates each Basic, Custom or Competition-period
`backtest.runs` row and `backtest.run_input_pins` row before its Outbox message.
Competition also creates the exact
`competition.backtest_period_runs` link first. Its v1 payload exposes the Custom
requesting account and the locked Competition period, datasets, policy and run
identity. `BacktestRequestIntake` verifies that envelope and its canonical Outbox
row, while `backtest_engine.production:backtest_request_handler` checks the
pre-created run and converts it to the smaller internal execution job.

Producer request queues and internal execution queues are different trust
boundaries and must be different SQS resources. Configure the backend relay to
the `_REQUEST_QUEUE_URL` queues below; keep `BACKTEST_{BASIC,CUSTOM,COMPETITION}_QUEUE_URL`
for the 2/1/1 execution scheduler. The worker refuses to start if any request,
request-DLQ or execution URL aliases another boundary.

When `BACKTEST_SCALE_DOWN_ENABLED=true`, scale-down is fail-closed unless all
three execution queues and all three producer request queues are configured.
The idle probe reads visible, in-flight and delayed counts from all six queues;
missing or invalid SQS telemetry resets the idle window and cannot authorize
desired capacity zero. CloudWatch request-queue alarms must wake the ASG from
zero before an intake worker exists.

```text
BACKTEST_BASIC_REQUEST_QUEUE_URL=...
BACKTEST_BASIC_REQUEST_DLQ_URL=...
BACKTEST_CUSTOM_REQUEST_QUEUE_URL=...
BACKTEST_CUSTOM_REQUEST_DLQ_URL=...
BACKTEST_COMPETITION_REQUEST_QUEUE_URL=...
BACKTEST_COMPETITION_REQUEST_DLQ_URL=...
BACKTEST_REQUEST_HANDLER=backtest_engine.production:backtest_request_handler
BACKTEST_REQUEST_RECEIPT_STORE=backtest_engine.production:postgres_request_receipt_store
```

Optional request-consumer controls are
`BACKTEST_REQUEST_MAX_RECEIVE_COUNT` (default 5),
`BACKTEST_REQUEST_VISIBILITY_TIMEOUT_SECONDS` (default 300), and
`BACKTEST_REQUEST_WAIT_SECONDS` (default 5). Never point a backend producer at
an execution queue: execution workers accept only internal jobs containing
`backtestRunId`, and raw Outbox envelopes are intentionally rejected.

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
`AWS_ENDPOINT_URL` is only for a local S3/SQS emulator. When local S3 and SQS
use different emulators, set `AWS_ENDPOINT_URL_S3` and
`AWS_ENDPOINT_URL_SQS`; each service-specific value takes precedence over the
shared fallback. For example, Compose points S3 at MinIO and SQS at LocalStack.

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

Validate the two approved files before uploading them:

```text
backtest-validate-deployment-artifacts \
  --execution-policy execution-policy.json \
  --runtime-policy runtime-policy.json
```

The command uses the same loaders as the API and worker, exits non-zero for an
invalid document, and emits stable JSON containing lowercase SHA-256 values for
the exact file bytes. Use those hashes with the version IDs returned after the
files are uploaded to the deployment artifact bucket. The Terraform artifact
map keys are `execution-policy` and `runtime-policy`; their runtime filenames
are `execution-policy.json` and `runtime-policy.json`. Do not reformat a file
after hashing or uploading it.

There is no separate backtest warmup artifact. Required warmup coverage comes
from the immutable compiled plan and is checked against the PostgreSQL dataset
manifest; the referenced market-data objects are fetched from
`BACKTEST_MARKET_DATA_BUCKET` and verified against each object's `content_hash`.
`BACKTEST_MARKET_DATA_CACHE` is only a private writable cache. A deployment-side
warmup manifest belongs to another runtime and must not be mounted as a backtest
policy input.

## Fenced-attempt schema gate

The central Flyway bundle now includes the expand/constrain pair
`V20260804160000__backtest_runtime_ownership_expand.sql` and
`V20260804160100__backtest_runtime_ownership_constrain.sql`. They add the opaque
claim token, claim expiry, heartbeat, cancellation state, and attempt-lineage
constraints used by the production persistence adapter.

`tests/persistence/test_fenced_attempts.py` verifies expired-claim replacement,
late-completion fencing, heartbeat renewal, and cancellation winning over a live
claim against the migrated PostgreSQL container. Deployment still must apply the
reviewed central bundle before starting this image; startup schema verification is
fail-closed and never applies DDL itself.
