"""SQLAlchemy Core table metadata for the canonical `backtest` schema.

Every definition here restates the applied central baseline
(`backend/db-migration/.../V1__initial_schema.sql`, generated from `db/schema.dbml`).
It is a *description* of a schema this process does not own the right to create:

* `create_type=False` on every enum, and this metadata is never used to emit DDL in a
  production path — the runtime creates nothing (COM07 `runtime-no-ddl`).
* Foreign keys to schemas outside `backtest` are declared with `use_alter=False` and
  are never used to emit DDL; they exist so joins and cascade reasoning are explicit.
* `storage.objects` is declared **read-only** here. `DatabaseAccessPolicy` registers
  the `storage` schema as SHARED while the implementation checklist calls it D-owned;
  until that is resolved this repository only reads it.

`tests/persistence/test_table_metadata.py` diffs this module against the canonical
DDL column by column, so a divergence fails without a database.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    Table,
    text,
)
from sqlalchemy.dialects.postgresql import CHAR, ENUM, JSONB, TIMESTAMP, UUID, VARCHAR


__all__ = [
    "BACKTEST_SCHEMA",
    "METADATA",
    "MONEY",
    "OBJECT_STATUS_LABELS",
    "RUN_STATUS_LABELS",
    "STORAGE_SCHEMA",
    "WORK_STATUS_LABELS",
    "detail_manifests",
    "failure_condition_counts",
    "input_bundles",
    "input_datasets",
    "input_feature_materializations",
    "monthly_judgment_summaries",
    "performance_summaries",
    "run_attempts",
    "run_input_pins",
    "runs",
    "storage_objects",
]


BACKTEST_SCHEMA = "backtest"
STORAGE_SCHEMA = "storage"
OPERATIONS_SCHEMA = "operations"

#: `backtest.run_status`. The pre-rebuild code used `COMPLETE`, which is not a label
#: of the canonical enum and would fail at insert time.
RUN_STATUS_LABELS: tuple[str, ...] = ("QUEUED", "RUNNING", "COMPLETED", "FAILED", "UNAVAILABLE")

#: `operations.work_status`, used by `backtest.run_attempts.status`.
WORK_STATUS_LABELS: tuple[str, ...] = (
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "SKIPPED",
)

#: `storage.object_status`.
OBJECT_STATUS_LABELS: tuple[str, ...] = (
    "STAGED",
    "AVAILABLE",
    "QUARANTINED",
    "SUPERSEDED",
    "DELETED",
)

#: Every monetary column in the canonical model. Quantising to 8 fractional digits is
#: `money.py`'s job; this type is the storage half of the same contract.
MONEY = Numeric(precision=24, scale=8, asdecimal=True)

METADATA = MetaData()


def _run_status() -> ENUM:
    return ENUM(
        *RUN_STATUS_LABELS,
        name="run_status",
        schema=BACKTEST_SCHEMA,
        create_type=False,
    )


def _work_status() -> ENUM:
    return ENUM(
        *WORK_STATUS_LABELS,
        name="work_status",
        schema=OPERATIONS_SCHEMA,
        create_type=False,
    )


def _object_status() -> ENUM:
    return ENUM(
        *OBJECT_STATUS_LABELS,
        name="object_status",
        schema=STORAGE_SCHEMA,
        create_type=False,
    )


runs = Table(
    "runs",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    # `bot.bots.id` and `identity.accounts.id`: read-only upstream references. They are
    # not declared as SQLAlchemy ForeignKeys because those tables are not in this
    # metadata and declaring them would pull foreign schemas into it.
    Column("bot_id", UUID(as_uuid=True), nullable=False),
    # Retention execution anonymizes historical competition owners after the
    # account is purged. Active owner-scoped queries still require a UUID, but
    # the persisted column must accept the canonical tombstone value (NULL).
    Column("owner_account_id", UUID(as_uuid=True), nullable=True),
    Column("configuration_hash", VARCHAR(128), nullable=False),
    Column("status", _run_status(), nullable=False),
    Column("evaluation_start", Date, nullable=False),
    Column("evaluation_end", Date, nullable=False),
    Column("initial_cash_amount", MONEY, nullable=False),
    Column("market_rules_version", VARCHAR(80), nullable=False),
    Column("accounting_rules_version", VARCHAR(80), nullable=False),
    Column("precision_rules_version", VARCHAR(80), nullable=False),
    Column("fee_policy_id", UUID(as_uuid=True), nullable=False),
    Column("slippage_rate_bps", Integer, nullable=False),
    Column("buying_power_buffer_policy_id", UUID(as_uuid=True), nullable=False),
    Column("idempotency_key", VARCHAR(160), nullable=False, unique=True),
    Column("queued_at", TIMESTAMP(timezone=True), nullable=False),
    Column("started_at", TIMESTAMP(timezone=True)),
    Column("completed_at", TIMESTAMP(timezone=True)),
    Column("failure_code", VARCHAR(80)),
    Column("result_hash", VARCHAR(128)),
    # Contributed by `db/migration-contributions/migrations/
    # V20260802143000__backtest_run_outcome_detail.sql`. Each belongs to exactly one
    # terminal status and is NULL in every other, so absence stays distinguishable
    # from a decision: `retryable IS NULL` means "this run has not failed", which is
    # not the same fact as `retryable = false`.
    Column("result_manifest_id", UUID(as_uuid=True)),
    Column("retryable", Boolean),
    # `none_as_null=True` is required, not stylistic. SQLAlchemy's JSON types default
    # to `none_as_null=False`, which stores Python `None` as the *JSON scalar* `null`
    # rather than as SQL NULL. That value is not NULL, so `missing_requirements IS
    # NULL` would be false for every run that has none, the CHECK contributed with
    # this column would reject every ordinary insert, and a reader would see the
    # string "null" where it expected absence.
    Column("missing_requirements", JSONB(none_as_null=True)),
    Column("owner_anonymized_at", TIMESTAMP(timezone=True)),
    Index("ix_runs_bot_id_queued_at", "bot_id", "queued_at"),
    Index("ix_runs_status_queued_at", "status", "queued_at"),
    Index("ix_runs_owner_account_id_queued_at", "owner_account_id", "queued_at"),
    schema=BACKTEST_SCHEMA,
)

run_attempts = Table(
    "run_attempts",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("attempt_number", Integer, nullable=False),
    # The cross-process duplicate-worker control. A second worker that derives the same
    # execution key cannot insert a second attempt row; an in-process lock cannot do this.
    Column("worker_execution_key", VARCHAR(160), nullable=False, unique=True),
    Column("status", _work_status(), nullable=False),
    Column("started_at", TIMESTAMP(timezone=True), nullable=False),
    Column("completed_at", TIMESTAMP(timezone=True)),
    Column("failure_code", VARCHAR(80)),
    Index("uq_run_attempts_run_id_attempt_number", "run_id", "attempt_number", unique=True),
    schema=BACKTEST_SCHEMA,
)

#: Contributed by this repository, not by the applied baseline:
#: `db/migration-contributions/migrations/V20260802094500__backtest_run_input_pins.sql`
#: plus the change request beside it. The four version/checksum columns are inputs of
#: `runs.configuration_hash`, which is a one-way digest, so `GET /{run_id}/inputs`
#: cannot recover them from any existing column. Written once, in the acceptance
#: transaction, so the route answers at `QUEUED` and `UNAVAILABLE` too.
run_input_pins = Table(
    "run_input_pins",
    METADATA,
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column("compiled_plan_checksum", VARCHAR(128), nullable=False),
    Column("strategy_snapshot_hash", VARCHAR(128), nullable=False),
    # `market_data.dataset_manifests.id`: read-only upstream reference, declared in the
    # migration exactly as `input_datasets.dataset_manifest_id` is.
    Column("dataset_manifest_id", UUID(as_uuid=True), nullable=False),
    Column("dataset_hash", VARCHAR(128), nullable=False),
    Column("feature_materialization_version", VARCHAR(80), nullable=False),
    Column("execution_policy_version", VARCHAR(80), nullable=False),
    Column("pinned_at", TIMESTAMP(timezone=True), nullable=False),
    schema=BACKTEST_SCHEMA,
)

input_bundles = Table(
    "input_bundles",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
        unique=True,
    ),
    Column("bundle_hash", VARCHAR(128), nullable=False),
    Column("as_of_at", TIMESTAMP(timezone=True), nullable=False),
    Column("locked_at", TIMESTAMP(timezone=True), nullable=False),
    schema=BACKTEST_SCHEMA,
)

input_datasets = Table(
    "input_datasets",
    METADATA,
    Column(
        "input_bundle_id",
        UUID(as_uuid=True),
        ForeignKey(input_bundles.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    # `market_data.dataset_manifests.id`: read-only upstream reference.
    Column("dataset_manifest_id", UUID(as_uuid=True), primary_key=True),
    Column("purpose_code", VARCHAR(80), primary_key=True),
    Column("locked_dataset_hash", VARCHAR(128), nullable=False),
    schema=BACKTEST_SCHEMA,
)

input_feature_materializations = Table(
    "input_feature_materializations",
    METADATA,
    Column(
        "input_bundle_id",
        UUID(as_uuid=True),
        ForeignKey(input_bundles.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    # `market_data.feature_materializations.id`: read-only upstream reference.
    Column("feature_materialization_id", UUID(as_uuid=True), primary_key=True),
    Column("locked_result_hash", VARCHAR(128), nullable=False),
    schema=BACKTEST_SCHEMA,
)

monthly_judgment_summaries = Table(
    "monthly_judgment_summaries",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("et_year_month", CHAR(7), nullable=False),
    Column("evaluation_count", BigInteger, nullable=False),
    Column("active_branch_count", BigInteger, nullable=False),
    Column("trade_event_count", BigInteger, nullable=False),
    Column("data_gap_count", BigInteger, nullable=False),
    Column("triggered_count", BigInteger, nullable=False),
    Column("rejected_count", BigInteger, nullable=False),
    Column("summary_document", JSONB, nullable=False),
    Column("summary_hash", VARCHAR(128), nullable=False),
    Index(
        "uq_monthly_judgment_summaries_run_id_et_year_month",
        "run_id",
        "et_year_month",
        unique=True,
    ),
    schema=BACKTEST_SCHEMA,
)

failure_condition_counts = Table(
    "failure_condition_counts",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "monthly_summary_id",
        UUID(as_uuid=True),
        ForeignKey(monthly_judgment_summaries.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("flow_or_branch_key", VARCHAR(160), nullable=False),
    Column("first_failure_condition_key", VARCHAR(160), nullable=False),
    Column("occurrence_count", BigInteger, nullable=False),
    Index(
        "uq_failure_condition_counts_summary_flow_condition",
        "monthly_summary_id",
        "flow_or_branch_key",
        "first_failure_condition_key",
        unique=True,
    ),
    schema=BACKTEST_SCHEMA,
)

performance_summaries = Table(
    "performance_summaries",
    METADATA,
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column("metric_catalog_version", VARCHAR(80), nullable=False),
    Column("metrics_document", JSONB, nullable=False),
    Column("calculation_rules_version", VARCHAR(80), nullable=False),
    Column("source_set_hash", VARCHAR(128), nullable=False),
    Column("input_hash", VARCHAR(128), nullable=False),
    Column("result_hash", VARCHAR(128), nullable=False),
    Column("calculated_at", TIMESTAMP(timezone=True), nullable=False),
    schema=BACKTEST_SCHEMA,
)

detail_manifests = Table(
    "detail_manifests",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "run_id",
        UUID(as_uuid=True),
        ForeignKey(runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    # `storage.objects.id`: one manifest per object, enforced by a unique index.
    Column("object_id", UUID(as_uuid=True), nullable=False),
    Column("record_type", VARCHAR(50), nullable=False),
    # ET Monday week boundary, not an ET month. `part_number` splits a week.
    Column("week_start_date", Date, nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True), nullable=False),
    Column("part_number", Integer, nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("schema_version", VARCHAR(40), nullable=False),
    Column("source_set_hash", VARCHAR(128), nullable=False),
    Column("supersedes_manifest_id", UUID(as_uuid=True)),
    Column("detail_hash", VARCHAR(128), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index(
        "uq_detail_manifests_run_record_week_part",
        "run_id",
        "record_type",
        "week_start_date",
        "part_number",
        unique=True,
    ),
    Index("uq_detail_manifests_object_id", "object_id", unique=True),
    schema=BACKTEST_SCHEMA,
)

#: Read-only. See the module docstring and
#: `db/migration-contributions/README.md` for the ownership contradiction.
storage_objects = Table(
    "objects",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("status", _object_status(), nullable=False),
    Column("storage_provider", VARCHAR(40), nullable=False),
    Column("bucket_name", VARCHAR(160), nullable=False),
    Column("object_key", VARCHAR(900), nullable=False),
    Column("provider_version_id", VARCHAR(300), nullable=False),
    Column("content_hash", VARCHAR(128), nullable=False),
    Column("byte_size", BigInteger, nullable=False),
    Column("file_format", VARCHAR(40), nullable=False),
    Column("compression_codec", VARCHAR(40), nullable=False),
    Column("media_type", VARCHAR(120), nullable=False),
    Column("schema_version", VARCHAR(40), nullable=False),
    Column("row_count", BigInteger),
    Column("period_start", TIMESTAMP(timezone=True)),
    Column("period_end", TIMESTAMP(timezone=True)),
    Column("encryption_key_ref", VARCHAR(300)),
    Column("retention_policy_version", VARCHAR(80), nullable=False),
    Column("retention_until", TIMESTAMP(timezone=True)),
    Column("legal_hold", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("verified_at", TIMESTAMP(timezone=True)),
    Column("quarantined_at", TIMESTAMP(timezone=True)),
    Column("superseded_at", TIMESTAMP(timezone=True)),
    Column("deleted_at", TIMESTAMP(timezone=True)),
    Index(
        "uq_storage_objects_provider_bucket_key_version",
        "storage_provider",
        "bucket_name",
        "object_key",
        "provider_version_id",
        unique=True,
    ),
    Index("ix_storage_objects_content_hash_byte_size", "content_hash", "byte_size"),
    Index("ix_storage_objects_status_created_at", "status", "created_at"),
    Index("ix_storage_objects_retention_until", "retention_until"),
    schema=STORAGE_SCHEMA,
)

#: Tables this repository may write, in foreign-key-safe insertion order.
WRITABLE_TABLES: tuple[Table, ...] = (
    runs,
    run_attempts,
    run_input_pins,
    input_bundles,
    input_datasets,
    input_feature_materializations,
    monthly_judgment_summaries,
    failure_condition_counts,
    performance_summaries,
    detail_manifests,
)

#: Tables this repository may only read.
READ_ONLY_TABLES: tuple[Table, ...] = (storage_objects,)
