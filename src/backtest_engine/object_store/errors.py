"""Failure modes of the object store, kept separate so callers can catch precisely."""

from __future__ import annotations


__all__ = [
    "ObjectKeyError",
    "ObjectStoreConflict",
    "ObjectStoreError",
    "ObjectVerificationError",
    "StorageWriteNotAuthorized",
]


class ObjectStoreError(RuntimeError):
    """Base class for every object-store failure raised by this package."""


class ObjectKeyError(ObjectStoreError, ValueError):
    """The object key is not a key this store will accept.

    Raised for a malformed canonical backtest key and for any key that would resolve
    outside the local store root (path traversal).
    """


class ObjectStoreConflict(ObjectStoreError):
    """The key already holds different bytes.

    Objects are immutable: a conflicting write is reconciled by inspecting what is
    already stored and then refused. It is never resolved by overwriting.
    """


class ObjectVerificationError(ObjectStoreError):
    """A stored object failed its post-write checksum or size verification."""


class StorageWriteNotAuthorized(ObjectStoreError):
    """A `storage.objects` row was offered to a write port that must not write it.

    See `registration.UnauthorizedStorageObjectWritePort` for why this is the default.
    """
