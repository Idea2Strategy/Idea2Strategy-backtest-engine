"""Immutable objects in S3-compatible storage.

Three things make this adapter safe to retry, which a plain `put_object` is not:

1. **`IfNoneMatch="*"`** makes the write conditional on the key not existing, so two
   writers racing on the same key cannot silently produce last-writer-wins. The loser
   gets HTTP 412.
2. **412 reconciliation.** A 412 is not an error by itself: for a content-addressed
   key it almost always means "somebody already wrote exactly these bytes", including
   *us*, on an attempt whose response was lost. The adapter HEADs the key and compares
   the stored SHA-256 and size. Equal means done (`reconciled=True`); different means
   `ObjectStoreConflict`. It never overwrites.
3. **Classified retries.** Only throttling and 5xx are retried, with exponential
   backoff through an injected sleeper so the schedule is deterministic and tests do
   not sleep. 4xx (auth, malformed request) fails immediately.

The SHA-256 travels in object metadata and `put` verifies it with a HEAD before
returning, so a receipt always describes bytes the server acknowledged.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Mapping
from typing import Any, BinaryIO, TypeVar

from .base import (
    PARQUET_MEDIA_TYPE,
    ObjectReceipt,
    Sleeper,
    VerificationResult,
    normalize_object_key,
    sha256_bytes,
)
from .errors import ObjectStoreConflict


__all__ = ["RETRYABLE_ERROR_CODES", "S3ObjectStore"]

_ResultT = TypeVar("_ResultT")

RETRYABLE_ERROR_CODES = frozenset(
    {
        "429",
        "500",
        "502",
        "503",
        "504",
        "InternalError",
        "RequestTimeout",
        "RequestTimeoutException",
        "ServiceUnavailable",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
    }
)
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_MISSING_CODES = frozenset({"404", "NoSuchKey", "NoSuchVersion", "NotFound"})
_PRECONDITION_CODES = frozenset({"412", "PreconditionFailed"})


def _sleep_for_real(seconds: float) -> None:  # pragma: no cover - replaced in tests
    import time

    time.sleep(seconds)


class S3ObjectStore:
    """S3-compatible immutable store. `client` is any boto3-shaped S3 client."""

    storage_provider = "S3_COMPATIBLE"

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        endpoint_url: str | None = None,
        client: Any | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 0.1,
        sleep: Sleeper = _sleep_for_real,
    ) -> None:
        if not bucket or not bucket.strip():
            raise ValueError("bucket must be a non-empty string")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self._sleep = sleep

    @property
    def bucket_name(self) -> str:
        return self.bucket

    # -- key handling ------------------------------------------------------------

    def full_key(self, object_key: str) -> str:
        normalized = normalize_object_key(object_key)
        if self.prefix and normalized.startswith(f"{self.prefix}/"):
            return normalized
        return "/".join(part for part in (self.prefix, normalized) if part)

    # -- error classification ----------------------------------------------------

    @staticmethod
    def _details(exc: BaseException) -> tuple[str, int | None]:
        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return "", None
        error = response.get("Error", {})
        metadata = response.get("ResponseMetadata", {})
        code = str(error.get("Code", "")) if isinstance(error, dict) else ""
        status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        return code, status if isinstance(status, int) else None

    @classmethod
    def is_missing(cls, exc: BaseException) -> bool:
        code, status = cls._details(exc)
        return status == 404 or code in _MISSING_CODES

    @classmethod
    def is_precondition_failed(cls, exc: BaseException) -> bool:
        code, status = cls._details(exc)
        return status == 412 or code in _PRECONDITION_CODES

    @classmethod
    def is_retryable(cls, exc: BaseException) -> bool:
        code, status = cls._details(exc)
        return status in _RETRYABLE_STATUS or code in RETRYABLE_ERROR_CODES

    def backoff_seconds(self, attempt: int) -> float:
        """Deterministic exponential backoff: delay * 2^(attempt-1)."""

        return self.retry_delay_seconds * (2 ** (attempt - 1))

    def _call_with_retries(self, operation: Any, **kwargs: Any) -> Any:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation(**kwargs)
            except Exception as exc:
                if attempt == self.max_attempts or not self.is_retryable(exc):
                    raise
                self._sleep(self.backoff_seconds(attempt))
        raise AssertionError("unreachable: the retry loop always returns or raises")

    # -- operations ---------------------------------------------------------------

    def _head(self, key: str) -> dict[str, Any]:
        head: dict[str, Any] = self._call_with_retries(self.client.head_object, Bucket=self.bucket, Key=key)
        return head

    def _receipt_from_head(
        self,
        *,
        key: str,
        expected_hash: str,
        expected_size: int,
        head: Mapping[str, Any],
        reconciled: bool,
    ) -> ObjectReceipt:
        metadata = head.get("Metadata", {})
        actual_hash = str(metadata.get("sha256", "")) if isinstance(metadata, Mapping) else ""
        actual_size = int(head.get("ContentLength", 0))
        etag = str(head.get("ETag", "")).strip('"') or None
        if actual_hash != expected_hash:
            raise ObjectStoreConflict(
                f"immutable object key already holds different bytes: s3://{self.bucket}/{key} "
                f"(stored sha256 {actual_hash or 'absent'}, offered {expected_hash})"
            )
        if actual_size != expected_size:
            raise ObjectStoreConflict(
                f"stored object byte size does not match: s3://{self.bucket}/{key} "
                f"(stored {actual_size}, offered {expected_size})"
            )
        version = head.get("VersionId") or etag
        if not version:  # pragma: no cover - every S3 server returns one of the two
            raise ObjectStoreConflict(
                f"server returned neither VersionId nor ETag for s3://{self.bucket}/{key}; "
                "storage.objects.provider_version_id is NOT NULL"
            )
        return ObjectReceipt(
            storage_provider=self.storage_provider,
            bucket_name=self.bucket,
            object_key=key,
            provider_version_id=str(version),
            content_hash=actual_hash,
            byte_size=actual_size,
            etag=etag,
            reconciled=reconciled,
        )

    def put(self, object_key: str, data: bytes) -> ObjectReceipt:
        if not isinstance(data, bytes):
            raise TypeError(f"object body must be bytes, got {type(data).__name__}")
        key = self.full_key(object_key)
        content_hash = sha256_bytes(data)
        byte_size = len(data)

        try:
            existing = self._head(key)
        except Exception as exc:
            if not self.is_missing(exc):
                raise
        else:
            return self._receipt_from_head(
                key=key,
                expected_hash=content_hash,
                expected_size=byte_size,
                head=existing,
                reconciled=True,
            )

        for attempt in range(1, self.max_attempts + 1):
            try:
                # A fresh stream every attempt: a failed SDK call may have consumed the
                # body even when the response never arrived.
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=io.BytesIO(data),
                    ContentLength=byte_size,
                    ContentType=PARQUET_MEDIA_TYPE,
                    IfNoneMatch="*",
                    Metadata={"sha256": content_hash},
                )
                break
            except Exception as exc:
                if self.is_precondition_failed(exc):
                    # Somebody wrote this key first — possibly a previous attempt of
                    # ours whose response was lost. Reconcile against what is stored.
                    return self._receipt_from_head(
                        key=key,
                        expected_hash=content_hash,
                        expected_size=byte_size,
                        head=self._head(key),
                        reconciled=True,
                    )
                if attempt == self.max_attempts or not self.is_retryable(exc):
                    raise
                self._sleep(self.backoff_seconds(attempt))

        return self._receipt_from_head(
            key=key,
            expected_hash=content_hash,
            expected_size=byte_size,
            head=self._head(key),
            reconciled=False,
        )

    def exists(self, object_key: str) -> bool:
        try:
            self._head(self.full_key(object_key))
        except Exception as exc:
            if self.is_missing(exc):
                return False
            raise
        return True

    def delete_if_matches(self, object_key: str, expected_sha256: str) -> bool:
        """Delete only the exact unpublished version whose digest was observed."""

        key = self.full_key(object_key)
        try:
            head = self._head(key)
        except Exception as exc:
            if self.is_missing(exc):
                return False
            raise
        metadata = head.get("Metadata", {})
        actual = str(metadata.get("sha256", "")) if isinstance(metadata, Mapping) else ""
        if actual != expected_sha256:
            raise ObjectStoreConflict(
                f"refusing to delete changed immutable object s3://{self.bucket}/{key}: "
                f"stored {actual or 'absent'}, expected {expected_sha256}"
            )
        version_id = head.get("VersionId")
        arguments: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if version_id:
            arguments["VersionId"] = str(version_id)
        self._call_with_retries(self.client.delete_object, **arguments)
        try:
            if version_id:
                self._call_with_retries(
                    self.client.head_object,
                    Bucket=self.bucket,
                    Key=key,
                    VersionId=str(version_id),
                )
            else:
                self._head(key)
        except Exception as exc:
            if self.is_missing(exc):
                return True
            raise
        raise ObjectStoreConflict(
            f"object store acknowledged delete but retained s3://{self.bucket}/{key}"
        )

    def open(self, object_key: str) -> BinaryIO:
        response = self._call_with_retries(
            self.client.get_object, Bucket=self.bucket, Key=self.full_key(object_key)
        )
        body: BinaryIO = response["Body"]
        return body

    def metadata(self, object_key: str) -> Mapping[str, str]:
        head = self._head(self.full_key(object_key))
        stored = head.get("Metadata", {})
        return {
            "sha256": str(stored.get("sha256", "")) if isinstance(stored, Mapping) else "",
            "content_type": str(head.get("ContentType", "")),
            "byte_size": str(int(head.get("ContentLength", 0))),
        }

    def verify(self, object_key: str, expected_sha256: str, *, deep: bool = False) -> VerificationResult:
        """Check the stored SHA-256.

        The default is the metadata check, which is one HEAD. `deep=True` downloads the
        body and re-hashes it, which is the only check that catches metadata that no
        longer describes the bytes.
        """

        key = self.full_key(object_key)
        try:
            head = self._head(key)
        except Exception as exc:
            if self.is_missing(exc):
                return VerificationResult(False, "", 0, f"object missing: {object_key}", deep=deep)
            raise

        byte_size = int(head.get("ContentLength", 0))
        if deep:
            digest = hashlib.sha256()
            body = self.open(object_key)
            try:
                downloaded = body.read()
            finally:
                body.close()
            digest.update(downloaded)
            actual = digest.hexdigest()
            byte_size = len(downloaded)
        else:
            stored = head.get("Metadata", {})
            actual = str(stored.get("sha256", "")) if isinstance(stored, Mapping) else ""

        matched = actual == expected_sha256
        return VerificationResult(
            matched,
            actual,
            byte_size,
            "" if matched else f"sha256 mismatch: stored {actual or 'absent'}, expected {expected_sha256}",
            deep=deep,
        )
