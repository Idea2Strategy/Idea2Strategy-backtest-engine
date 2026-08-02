"""Durable persistence for the canonical `backtest` schema, in SQLAlchemy Core.

Nothing in this package applies DDL. The schema is created once by the central Flyway
bundle in `backend/db-migration`; the runtime verifies it with
`BacktestPersistence.verify_schema()` and refuses to start on drift.

Typical use::

    engine = create_backtest_engine(os.environ["BACKTEST_DATABASE_URL"])
    persistence = BacktestPersistence(engine)
    persistence.verify_schema()

    with persistence.unit_of_work() as uow:
        run, created = uow.runs.accept(run_row)

`unit_of_work()` is one transaction: a multi-table publish either lands completely or
not at all.
"""

from __future__ import annotations

from .contribution import (
    CENTRAL_MIGRATION_OWNERS,
    CENTRAL_SCHEMA_OWNERS,
    RUNTIME_ROW_ONLY_SCHEMAS,
    ContributionError,
    MigrationContribution,
    load_contribution,
)
from .engine import (
    BacktestPersistence,
    bind,
    check_statement,
    create_backtest_engine,
    install_runtime_guards,
)
from .errors import (
    AttemptNumberConflict,
    DuplicateWorkerExecution,
    IdempotencyConflict,
    InvalidStatusTransition,
    MoneyPrecisionError,
    PersistenceError,
    PublishConflict,
    RowNotFound,
    RuntimeDdlForbidden,
    SchemaDriftError,
    SchemaWriteForbidden,
)
from .publish import (
    MonthlyJudgment,
    RunPublication,
    publish_completed_run,
    publish_failed_run,
    sum_monthly_counters,
)
from .repositories import (
    BacktestUnitOfWork,
    DetailManifestRepository,
    InputBundleRepository,
    MonthlyJudgmentRepository,
    PerformanceSummaryRepository,
    RunAttemptRepository,
    RunRepository,
    StorageObjectReader,
    StorageObjectRepository,
)
from .rows import (
    DetailManifestRow,
    FailureConditionCountRow,
    InputBundleRow,
    InputDatasetRow,
    InputFeatureMaterializationRow,
    MonthlyJudgmentSummaryRow,
    ObjectStatus,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunRow,
    RunStatus,
    StorageObjectRow,
    WorkStatus,
    validate_money,
)
from .schema_guard import describe_schema_drift, verify_schema
from .tables import METADATA


__all__ = [
    "CENTRAL_MIGRATION_OWNERS",
    "CENTRAL_SCHEMA_OWNERS",
    "METADATA",
    "RUNTIME_ROW_ONLY_SCHEMAS",
    "AttemptNumberConflict",
    "BacktestPersistence",
    "BacktestUnitOfWork",
    "ContributionError",
    "DetailManifestRepository",
    "DetailManifestRow",
    "DuplicateWorkerExecution",
    "FailureConditionCountRow",
    "IdempotencyConflict",
    "InputBundleRepository",
    "InputBundleRow",
    "InputDatasetRow",
    "InputFeatureMaterializationRow",
    "InvalidStatusTransition",
    "MigrationContribution",
    "MoneyPrecisionError",
    "MonthlyJudgment",
    "MonthlyJudgmentRepository",
    "MonthlyJudgmentSummaryRow",
    "ObjectStatus",
    "PerformanceSummaryRepository",
    "PerformanceSummaryRow",
    "PersistenceError",
    "PublishConflict",
    "RowNotFound",
    "RunAttemptRepository",
    "RunAttemptRow",
    "RunPublication",
    "RunRepository",
    "RunRow",
    "RunStatus",
    "RuntimeDdlForbidden",
    "SchemaDriftError",
    "SchemaWriteForbidden",
    "StorageObjectReader",
    "StorageObjectRepository",
    "StorageObjectRow",
    "WorkStatus",
    "bind",
    "check_statement",
    "create_backtest_engine",
    "describe_schema_drift",
    "install_runtime_guards",
    "load_contribution",
    "publish_completed_run",
    "publish_failed_run",
    "sum_monthly_counters",
    "validate_money",
    "verify_schema",
]
