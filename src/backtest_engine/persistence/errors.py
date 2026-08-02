"""Exception hierarchy for the durable persistence layer.

`MoneyPrecisionError` is deliberately **not** defined here. It is re-exported from
`backtest_engine.money`, which owns the `precision:1.0.0` rules. Two quantisation
error types would drift apart exactly the way the two COM06 contract fixtures did,
and a caller that catches one but not the other would silently let an unquantised
value reach `numeric(24,8)`.
"""

from __future__ import annotations

from backtest_engine.money import MoneyPrecisionError


__all__ = [
    "AttemptNumberConflict",
    "DuplicateWorkerExecution",
    "IdempotencyConflict",
    "InvalidStatusTransition",
    "MoneyPrecisionError",
    "PersistenceError",
    "PublishConflict",
    "RowNotFound",
    "RuntimeDdlForbidden",
    "SchemaDriftError",
    "SchemaWriteForbidden",
]


class PersistenceError(Exception):
    """Base class for every failure raised by `backtest_engine.persistence`."""


class RowNotFound(PersistenceError, LookupError):
    """A required row does not exist, or is not visible to the requesting owner."""


class IdempotencyConflict(PersistenceError, ValueError):
    """An idempotency key was reused for a materially different request."""


class DuplicateWorkerExecution(PersistenceError, RuntimeError):
    """`run_attempts.worker_execution_key` is already claimed by another process."""


class AttemptNumberConflict(PersistenceError, RuntimeError):
    """`(run_id, attempt_number)` is already claimed by a different execution key."""


class InvalidStatusTransition(PersistenceError, ValueError):
    """A conditional status update did not match the row's current status."""


class PublishConflict(PersistenceError, RuntimeError):
    """An atomic publish contradicts already-published, immutable result rows."""


class SchemaDriftError(PersistenceError, RuntimeError):
    """The live database does not match the schema this code was written against."""


class RuntimeDdlForbidden(PersistenceError, RuntimeError):
    """A runtime connection attempted DDL. Migrations belong to the central bundle."""


class SchemaWriteForbidden(PersistenceError, RuntimeError):
    """A runtime connection attempted to write a schema this repository does not own."""
