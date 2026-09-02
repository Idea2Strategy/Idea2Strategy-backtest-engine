"""Contract tests for the backtest object store (spec 2.5, card D03/BT3).

The same contract functions (`assert_*`) run against every adapter:

* `LocalObjectStore` on a real filesystem,
* `S3ObjectStore` against an in-process S3 model (`_FakeS3`), and
* `S3ObjectStore` against a real S3 emulator in the Docker suite at the bottom.

The in-process model exists so the retry, backoff and 412-reconciliation paths can
be driven deterministically (no sleeping, no flakiness). It is not the only witness:
every contract function it satisfies is also run against LocalStack, so a divergence
between the model and a real server shows up as a Docker-suite failure rather than as
a green unit suite.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from backtest_engine.object_store import (
    BACKTEST_RESULT_PREFIX,
    PARQUET_MEDIA_TYPE,
    RETENTION_POLICY_VERSION,
    UNCOMPRESSED_CODEC,
    BacktestObjectKey,
    InMemoryStorageObjectRegistry,
    LocalObjectStore,
    ObjectKeyError,
    ObjectReceipt,
    ObjectStore,
    ObjectStoreConflict,
    ObjectVerificationError,
    S3ObjectStore,
    StorageObjectRecord,
    StorageObjectRegistrar,
    StorageWriteNotAuthorized,
    UnauthorizedStorageObjectWritePort,
    long_path,
)
from backtest_engine.persistence import ObjectStatus, StorageObjectRow


RUN_ID = "00000000-0000-4000-8000-000000001001"
OBJECT_ID = UUID("00000000-0000-4000-8000-0000000000c1")
BODY = b"PAR1-example-body"
BODY_SHA256 = hashlib.sha256(BODY).hexdigest()
OTHER_BODY = b"PAR1-different-body"
OTHER_SHA256 = hashlib.sha256(OTHER_BODY).hexdigest()

#: A Monday. The canonical ET detail partition boundary.
WEEK_START = date(2025, 10, 27)

KEY = BacktestObjectKey(
    run_id=RUN_ID,
    record_type="TRADE_DETAIL",
    week_start=WEEK_START,
    part_number=1,
    content_hash=BODY_SHA256,
).render()


# --------------------------------------------------------------------------------
# key contract
# --------------------------------------------------------------------------------


def test_backtest_object_key_renders_the_canonical_spec_2_5_layout() -> None:
    key = BacktestObjectKey(
        run_id=RUN_ID,
        record_type="REPLAY_LEDGER",
        week_start=WEEK_START,
        part_number=42,
        content_hash="0" * 64,
    )

    assert key.render() == (
        "backtest-results/00000000-0000-4000-8000-000000001001/REPLAY_LEDGER/"
        "week_start=2025-10-27/part=0042/"
        "0000000000000000000000000000000000000000000000000000000000000000.parquet"
    )
    assert BACKTEST_RESULT_PREFIX == "backtest-results"
    assert BacktestObjectKey.parse(key.render()) == key


def test_backtest_object_key_rejects_every_non_canonical_component() -> None:
    def build(**overrides: Any) -> BacktestObjectKey:
        values: dict[str, Any] = {
            "run_id": RUN_ID,
            "record_type": "TRADE_DETAIL",
            "week_start": WEEK_START,
            "part_number": 1,
            "content_hash": BODY_SHA256,
        }
        values.update(overrides)
        return BacktestObjectKey(**values)

    with pytest.raises(ObjectKeyError, match="run_id"):
        build(run_id="not-a-uuid")
    with pytest.raises(ObjectKeyError, match="record_type"):
        build(record_type="trade detail")
    with pytest.raises(ObjectKeyError, match="Monday"):
        build(week_start=date(2025, 10, 28))
    with pytest.raises(ObjectKeyError, match="part_number"):
        build(part_number=0)
    with pytest.raises(ObjectKeyError, match="part_number"):
        build(part_number=10_000)
    with pytest.raises(ObjectKeyError, match="content_hash"):
        build(content_hash="ABC")


def test_backtest_object_key_parse_rejects_foreign_keys() -> None:
    for text in (
        "market-data/provider=X/dataset=Y/part.parquet",
        f"backtest-results/{RUN_ID}/TRADE_DETAIL/week_start=2025-10-27/{BODY_SHA256}.parquet",
        f"backtest-results/{RUN_ID}/TRADE_DETAIL/2025-10-27/part=0001/{BODY_SHA256}.parquet",
        f"backtest-results/{RUN_ID}/TRADE_DETAIL/week_start=2025-10-27/part=1/{BODY_SHA256}.parquet",
        f"backtest-results/{RUN_ID}/TRADE_DETAIL/week_start=2025-10-27/part=0001/{BODY_SHA256}.csv",
    ):
        with pytest.raises(ObjectKeyError):
            BacktestObjectKey.parse(text)


# --------------------------------------------------------------------------------
# the in-process S3 model
# --------------------------------------------------------------------------------


def _client_error(code: str, status: int, operation: str = "PutObject") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


class _FakeS3:
    """A deliberately small S3 model: conditional put, head, get, and injected faults.

    `put_faults` is a queue consumed one entry per `put_object` call. `None` means the
    call succeeds. An exception means the call raises *after* optionally storing the
    object, which is how a lost response is modelled (`store_then_fail`).
    """

    def __init__(self, *, store_then_fail: bool = False) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.put_faults: list[BaseException | None] = []
        self.put_calls = 0
        self.head_calls = 0
        self.store_then_fail = store_then_fail

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls += 1
        fault = self.put_faults.pop(0) if self.put_faults else None
        key = kwargs["Key"]
        stored = {
            "Body": kwargs["Body"].read() if hasattr(kwargs["Body"], "read") else kwargs["Body"],
            "Metadata": dict(kwargs.get("Metadata", {})),
            "ContentType": kwargs.get("ContentType"),
            "VersionId": f"v{len(self.objects) + 1}",
        }
        if fault is not None:
            if self.store_then_fail:
                self.objects.setdefault(key, stored)
            raise fault
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise _client_error("PreconditionFailed", 412)
        self.objects[key] = stored
        return {"VersionId": stored["VersionId"], "ETag": '"etag"'}

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        self.head_calls += 1
        try:
            stored = self.objects[Key]
        except KeyError as exc:
            raise _client_error("404", 404, "HeadObject") from exc
        return {
            "ContentLength": len(stored["Body"]),
            "Metadata": stored["Metadata"],
            "VersionId": stored["VersionId"],
            "ETag": '"etag"',
            "ContentType": stored["ContentType"],
        }

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        try:
            stored = self.objects[Key]
        except KeyError as exc:
            raise _client_error("NoSuchKey", 404, "GetObject") from exc
        return {"Body": io.BytesIO(stored["Body"]), "ContentLength": len(stored["Body"])}


class _VersionedFakeS3:
    """Small versioned S3 model that records every exact-version operation."""

    def __init__(self) -> None:
        self.versions: dict[str, list[dict[str, Any]]] = {}
        self.version_operations: list[tuple[str, str | None]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key = str(kwargs["Key"])
        versions = self.versions.setdefault(key, [])
        if kwargs.get("IfNoneMatch") == "*" and versions:
            raise _client_error("PreconditionFailed", 412)
        version_id = f"v{sum(len(items) for items in self.versions.values()) + 1}"
        stored = {
            "Body": kwargs["Body"].read() if hasattr(kwargs["Body"], "read") else kwargs["Body"],
            "Metadata": dict(kwargs.get("Metadata", {})),
            "ContentType": kwargs.get("ContentType"),
            "VersionId": version_id,
        }
        versions.append(stored)
        return {"VersionId": version_id, "ETag": f'"{version_id}-etag"'}

    def _version(self, key: str, version_id: str | None) -> dict[str, Any]:
        versions = self.versions.get(key, [])
        if version_id is None and versions:
            return versions[-1]
        for stored in versions:
            if stored["VersionId"] == version_id:
                return stored
        raise _client_error("NoSuchVersion", 404, "HeadObject")

    def head_object(
        self, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, Any]:
        self.version_operations.append(("HEAD", VersionId))
        stored = self._version(Key, VersionId)
        return {
            "ContentLength": len(stored["Body"]),
            "Metadata": stored["Metadata"],
            "VersionId": stored["VersionId"],
            "ETag": f'"{stored["VersionId"]}-etag"',
            "ContentType": stored["ContentType"],
        }

    def get_object(
        self, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, Any]:
        self.version_operations.append(("GET", VersionId))
        stored = self._version(Key, VersionId)
        return {
            "Body": io.BytesIO(stored["Body"]),
            "ContentLength": len(stored["Body"]),
        }

    def delete_object(
        self, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, Any]:
        self.version_operations.append(("DELETE", VersionId))
        versions = self.versions.get(Key, [])
        self.versions[Key] = [item for item in versions if item["VersionId"] != VersionId]
        return {"DeleteMarker": False, "VersionId": VersionId}


# --------------------------------------------------------------------------------
# shared contract, run against every adapter
# --------------------------------------------------------------------------------


def assert_put_returns_a_registrable_receipt(store: ObjectStore) -> None:
    receipt = store.put(KEY, BODY)

    assert receipt.storage_provider == store.storage_provider
    assert receipt.bucket_name == store.bucket_name
    assert receipt.object_key.endswith(KEY)
    assert receipt.content_hash == BODY_SHA256
    assert receipt.byte_size == len(BODY)
    assert receipt.provider_version_id
    assert receipt.reconciled is False


def assert_put_is_idempotent_for_identical_bytes(store: ObjectStore) -> None:
    first = store.put(KEY, BODY)
    second = store.put(KEY, BODY)

    assert second.content_hash == first.content_hash
    assert second.byte_size == first.byte_size
    assert second.reconciled is True
    assert store.open(KEY).read() == BODY


def assert_put_refuses_to_overwrite_different_bytes(store: ObjectStore) -> None:
    store.put(KEY, BODY)

    with pytest.raises(ObjectStoreConflict):
        store.put(KEY, OTHER_BODY)

    assert store.open(KEY).read() == BODY


def assert_exists_and_verify_report_the_truth(store: ObjectStore) -> None:
    assert store.exists(KEY) is False
    missing = store.verify(KEY, BODY_SHA256)
    assert missing.ok is False
    assert "missing" in missing.message

    store.put(KEY, BODY)

    assert store.exists(KEY) is True
    good = store.verify(KEY, BODY_SHA256)
    assert good.ok is True
    assert good.content_hash == BODY_SHA256
    assert good.byte_size == len(BODY)

    bad = store.verify(KEY, OTHER_SHA256)
    assert bad.ok is False
    assert bad.content_hash == BODY_SHA256


def assert_deep_verify_rehashes_the_body(store: ObjectStore) -> None:
    store.put(KEY, BODY)

    result = store.verify(KEY, BODY_SHA256, deep=True)

    assert result.ok is True
    assert result.deep is True
    assert result.content_hash == BODY_SHA256


def assert_metadata_carries_the_content_hash(store: ObjectStore) -> None:
    store.put(KEY, BODY)

    metadata = store.metadata(KEY)

    assert metadata["sha256"] == BODY_SHA256
    assert metadata["content_type"] == PARQUET_MEDIA_TYPE


def assert_registrar_registers_exactly_one_available_row(store: ObjectStore) -> None:
    """Publish -> STAGED row -> deep verify -> AVAILABLE, identically on every adapter."""

    registry = InMemoryStorageObjectRegistry()
    registrar = StorageObjectRegistrar(store, registry)

    def publish() -> Any:
        return registrar.publish(
            object_id=OBJECT_ID,
            object_key=KEY,
            data=BODY,
            schema_version="1.0.0",
            row_count=3,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            created_at=CREATED_AT,
            verified_at=VERIFIED_AT,
            expected_content_hash=BODY_SHA256,
        )

    first = publish()
    second = publish()

    assert first.record.status is ObjectStatus.AVAILABLE
    assert first.record.verified_at == VERIFIED_AT
    assert first.record.content_hash == BODY_SHA256
    assert first.record.byte_size == len(BODY)
    assert first.record.compression_codec == UNCOMPRESSED_CODEC
    assert first.record.storage_provider == store.storage_provider
    assert first.record.bucket_name == store.bucket_name
    assert second.record == first.record, "a re-publish reconciles onto the same row"
    assert registry.rows() == (first.record,), "exactly one storage.objects row"
    assert registry.register_calls == 2, "idempotent at the row, not at the call"
    assert registry.find(OBJECT_ID) == first.record


CONTRACT: tuple[Callable[[ObjectStore], None], ...] = (
    assert_put_returns_a_registrable_receipt,
    assert_put_is_idempotent_for_identical_bytes,
    assert_put_refuses_to_overwrite_different_bytes,
    assert_exists_and_verify_report_the_truth,
    assert_deep_verify_rehashes_the_body,
    assert_metadata_carries_the_content_hash,
    assert_registrar_registers_exactly_one_available_row,
)


@pytest.fixture(params=["local", "s3-model"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> ObjectStore:
    if request.param == "local":
        return LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    return S3ObjectStore("backtest-bucket", client=_FakeS3(), sleep=_no_sleep)


def _no_sleep(_seconds: float) -> None:
    raise AssertionError("a test must never sleep for real")


@pytest.mark.parametrize("contract", CONTRACT, ids=lambda item: item.__name__)
def test_object_store_contract(contract: Callable[[ObjectStore], None], store: ObjectStore) -> None:
    contract(store)


def test_both_adapters_satisfy_the_protocol(tmp_path: Path) -> None:
    local = LocalObjectStore(tmp_path, bucket_name="b")
    s3 = S3ObjectStore("b", client=_FakeS3(), sleep=_no_sleep)

    assert isinstance(local, ObjectStore)
    assert isinstance(s3, ObjectStore)
    assert local.storage_provider == "LOCAL"
    assert s3.storage_provider == "S3_COMPATIBLE"


# --------------------------------------------------------------------------------
# local adapter specifics
# --------------------------------------------------------------------------------


def test_local_store_writes_the_object_at_the_key_path(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    store = LocalObjectStore(root, bucket_name="backtest-local")

    receipt = store.put(KEY, BODY)

    # `long_path` is not a convenience here: a canonical key is ~160 characters, so
    # under a normal Windows temp root the object path is already past MAX_PATH and a
    # plain `Path.read_bytes()` raises FileNotFoundError on a file that exists.
    written = Path(long_path(root.joinpath(*KEY.split("/"))))
    assert written.read_bytes() == BODY
    assert receipt.local_path is not None
    assert Path(receipt.local_path).name == f"{BODY_SHA256}.parquet"
    assert receipt.local_path.endswith(str(Path(*KEY.split("/"))))
    assert receipt.provider_version_id == BODY_SHA256


def test_local_store_leaves_no_staging_file_behind(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    store = LocalObjectStore(root, bucket_name="backtest-local")

    store.put(KEY, BODY)

    leftovers = [path.name for path in Path(long_path(root)).rglob("*.tmp")]
    assert leftovers == []


@pytest.mark.parametrize(
    "malicious",
    [
        "../escaped.parquet",
        "backtest-results/../../escaped.parquet",
        "/absolute/escaped.parquet",
        "C:/Windows/escaped.parquet",
        "..\\escaped.parquet",
        "backtest-results\\..\\..\\escaped.parquet",
        "backtest-results//escaped.parquet",
        "backtest-results/./escaped.parquet",
        "backtest-results/nul\x00.parquet",
        "backtest-results/stream.parquet:ads",
        "",
        "   ",
    ],
)
def test_local_store_rejects_path_traversal_without_touching_the_filesystem(
    tmp_path: Path, malicious: str
) -> None:
    """The sibling pipeline shipped this guard untested for months. This is the test."""

    root = tmp_path / "objects"
    root.mkdir()
    outside = tmp_path / "escaped.parquet"
    store = LocalObjectStore(root, bucket_name="backtest-local")

    with pytest.raises(ObjectKeyError):
        store.put(malicious, BODY)
    with pytest.raises(ObjectKeyError):
        store.exists(malicious)
    with pytest.raises(ObjectKeyError):
        store.verify(malicious, BODY_SHA256)

    assert outside.exists() is False
    assert list(root.rglob("*")) == []


def test_local_store_detects_bytes_changed_underneath_it(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    store = LocalObjectStore(root, bucket_name="backtest-local")
    store.put(KEY, BODY)
    Path(long_path(root.joinpath(*KEY.split("/")))).write_bytes(OTHER_BODY)

    result = store.verify(KEY, BODY_SHA256)

    assert result.ok is False
    assert result.content_hash == OTHER_SHA256
    assert "sha256" in result.message
    with pytest.raises(ObjectStoreConflict):
        store.put(KEY, BODY)


def test_local_store_root_is_created_lazily_and_is_absolute(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "not-yet", bucket_name="backtest-local")

    assert store.root.is_absolute()
    assert store.root.exists() is False

    store.put(KEY, BODY)

    assert store.root.is_dir()


# --------------------------------------------------------------------------------
# S3 adapter specifics: retry classification, backoff, 412 reconciliation
# --------------------------------------------------------------------------------


def test_s3_retries_transient_failures_with_exponential_backoff() -> None:
    fake = _FakeS3()
    fake.put_faults = [_client_error("SlowDown", 503), _client_error("InternalError", 500)]
    delays: list[float] = []
    store = S3ObjectStore(
        "bucket", client=fake, max_attempts=3, retry_delay_seconds=0.25, sleep=delays.append
    )

    receipt = store.put(KEY, BODY)

    assert receipt.content_hash == BODY_SHA256
    assert fake.put_calls == 3
    assert delays == [0.25, 0.5]


def test_s3_does_not_retry_a_permanent_failure() -> None:
    fake = _FakeS3()
    fake.put_faults = [_client_error("AccessDenied", 403)]
    delays: list[float] = []
    store = S3ObjectStore("bucket", client=fake, max_attempts=3, retry_delay_seconds=0.25, sleep=delays.append)

    with pytest.raises(ClientError):
        store.put(KEY, BODY)

    assert fake.put_calls == 1
    assert delays == []


def test_s3_gives_up_after_max_attempts() -> None:
    fake = _FakeS3()
    fake.put_faults = [_client_error("SlowDown", 503) for _ in range(4)]
    delays: list[float] = []
    store = S3ObjectStore("bucket", client=fake, max_attempts=3, retry_delay_seconds=0.1, sleep=delays.append)

    with pytest.raises(ClientError):
        store.put(KEY, BODY)

    assert fake.put_calls == 3
    assert delays == [0.1, 0.2]


def test_s3_reconciles_a_lost_response_instead_of_overwriting() -> None:
    """The write landed, the response did not. The retry must 412 and reconcile."""

    fake = _FakeS3(store_then_fail=True)
    fake.put_faults = [_client_error("RequestTimeout", 500)]
    delays: list[float] = []
    store = S3ObjectStore("bucket", client=fake, max_attempts=3, retry_delay_seconds=0.1, sleep=delays.append)

    receipt = store.put(KEY, BODY)

    assert fake.put_calls == 2
    assert delays == [0.1]
    assert receipt.reconciled is True
    assert receipt.content_hash == BODY_SHA256
    assert store.open(KEY).read() == BODY


def test_s3_head_before_put_detects_an_existing_different_object() -> None:
    fake = _FakeS3()
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)
    store.put(KEY, BODY)
    fake.objects[KEY]["Body"] = OTHER_BODY
    fake.objects[KEY]["Metadata"] = {"sha256": OTHER_SHA256}

    with pytest.raises(ObjectStoreConflict, match="different bytes"):
        store.put(KEY, BODY)

    assert fake.put_calls == 1, "a conflict must never reach a second put_object"


def test_s3_precondition_failure_on_different_bytes_is_a_conflict() -> None:
    """Another writer won the race with *different* bytes between our HEAD and our PUT."""

    fake = _FakeS3()
    fake.objects[KEY] = {
        "Body": OTHER_BODY,
        "Metadata": {"sha256": OTHER_SHA256},
        "ContentType": PARQUET_MEDIA_TYPE,
        "VersionId": "v9",
    }
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)
    original_head = fake.head_object
    misses = [True]

    def head_missing_once(**kwargs: Any) -> dict[str, Any]:
        if misses:
            misses.pop()
            raise _client_error("404", 404, "HeadObject")
        return original_head(**kwargs)

    fake.head_object = head_missing_once  # type: ignore[method-assign]

    with pytest.raises(ObjectStoreConflict, match="different bytes"):
        store.put(KEY, BODY)

    assert fake.objects[KEY]["Body"] == OTHER_BODY, "the racer's object must survive"


def test_s3_put_sends_the_immutability_precondition_and_sha_metadata() -> None:
    recorded: dict[str, Any] = {}
    fake = _FakeS3()
    original = fake.put_object

    def capture(**kwargs: Any) -> dict[str, Any]:
        recorded.update(kwargs)
        return original(**kwargs)

    fake.put_object = capture  # type: ignore[method-assign]
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)

    store.put(KEY, BODY)

    assert recorded["IfNoneMatch"] == "*"
    assert recorded["Metadata"] == {"sha256": BODY_SHA256}
    assert recorded["ContentType"] == PARQUET_MEDIA_TYPE
    assert recorded["ContentLength"] == len(BODY)


def test_s3_rejects_a_server_that_stored_something_else() -> None:
    fake = _FakeS3()
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)

    def lying_put(**kwargs: Any) -> dict[str, Any]:
        fake.objects[kwargs["Key"]] = {
            "Body": OTHER_BODY,
            "Metadata": {"sha256": BODY_SHA256},
            "ContentType": PARQUET_MEDIA_TYPE,
            "VersionId": "v1",
        }
        return {"VersionId": "v1"}

    fake.put_object = lying_put  # type: ignore[method-assign]

    with pytest.raises(ObjectStoreConflict, match="byte size"):
        store.put(KEY, BODY)


def test_s3_deep_verify_catches_metadata_that_lies_about_the_body() -> None:
    fake = _FakeS3()
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)
    store.put(KEY, BODY)
    fake.objects[KEY]["Body"] = OTHER_BODY

    shallow = store.verify(KEY, BODY_SHA256)
    deep = store.verify(KEY, BODY_SHA256, deep=True)

    assert shallow.ok is True, "metadata still claims the original hash"
    assert deep.ok is False
    assert deep.content_hash == OTHER_SHA256


@pytest.mark.parametrize("newer_body", [BODY, OTHER_BODY], ids=["identical-hash", "changed"])
def test_s3_compensation_deletes_only_the_registered_provider_version(
    newer_body: bytes,
) -> None:
    fake = _VersionedFakeS3()
    store = S3ObjectStore("bucket", client=fake, sleep=_no_sleep)
    receipt = store.put(KEY, BODY)
    newer_hash = hashlib.sha256(newer_body).hexdigest()
    newer = fake.put_object(
        Bucket="bucket",
        Key=KEY,
        Body=newer_body,
        Metadata={"sha256": newer_hash},
        ContentType=PARQUET_MEDIA_TYPE,
    )

    assert store.preflight_delete(
        KEY,
        BODY_SHA256,
        receipt.provider_version_id,
    ) is True
    assert store.delete_if_matches(
        KEY,
        BODY_SHA256,
        receipt.provider_version_id,
    ) is True

    assert fake.get_object(Bucket="bucket", Key=KEY)["Body"].read() == newer_body
    assert fake.head_object(Bucket="bucket", Key=KEY)["VersionId"] == newer["VersionId"]
    assert ("DELETE", receipt.provider_version_id) in fake.version_operations
    assert all(
        version_id in {None, receipt.provider_version_id, newer["VersionId"]}
        for _operation, version_id in fake.version_operations
    )


def test_local_compensation_refuses_a_provider_version_mismatch(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    receipt = store.put(KEY, BODY)

    with pytest.raises(ObjectStoreConflict, match="provider version"):
        store.preflight_delete(KEY, BODY_SHA256, "different-provider-version")

    assert receipt.provider_version_id == BODY_SHA256
    assert store.open(KEY).read() == BODY


def test_s3_prefix_is_applied_once() -> None:
    fake = _FakeS3()
    store = S3ObjectStore("bucket", prefix="tenant-a/", client=fake, sleep=_no_sleep)

    receipt = store.put(KEY, BODY)

    assert receipt.object_key == f"tenant-a/{KEY}"
    assert list(fake.objects) == [f"tenant-a/{KEY}"]
    assert store.put(KEY, BODY).object_key == f"tenant-a/{KEY}"
    assert list(fake.objects) == [f"tenant-a/{KEY}"]


def test_s3_rejects_invalid_construction() -> None:
    with pytest.raises(ValueError, match="bucket"):
        S3ObjectStore("", client=_FakeS3())
    with pytest.raises(ValueError, match="max_attempts"):
        S3ObjectStore("bucket", client=_FakeS3(), max_attempts=0)
    with pytest.raises(ValueError, match="retry_delay_seconds"):
        S3ObjectStore("bucket", client=_FakeS3(), retry_delay_seconds=-1)


def test_s3_adapter_also_rejects_a_traversing_key() -> None:
    store = S3ObjectStore("bucket", client=_FakeS3(), sleep=_no_sleep)

    with pytest.raises(ObjectKeyError):
        store.put("../escaped.parquet", BODY)


# --------------------------------------------------------------------------------
# storage.objects row value
# --------------------------------------------------------------------------------


PERIOD_START = datetime(2025, 10, 27, 13, 30, tzinfo=UTC)
PERIOD_END = datetime(2025, 10, 31, 20, 0, tzinfo=UTC)
CREATED_AT = datetime(2025, 11, 1, 5, 0, tzinfo=UTC)
VERIFIED_AT = datetime(2025, 11, 1, 5, 0, 1, tzinfo=UTC)


def _receipt() -> ObjectReceipt:
    return ObjectReceipt(
        storage_provider="S3_COMPATIBLE",
        bucket_name="idea2strategy-backtest",
        object_key=KEY,
        provider_version_id="version-1",
        content_hash=BODY_SHA256,
        byte_size=len(BODY),
    )


def _record() -> StorageObjectRecord:
    return StorageObjectRecord.staged(
        object_id=OBJECT_ID,
        receipt=_receipt(),
        schema_version="1.0.0",
        row_count=3,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        created_at=CREATED_AT,
    )


def test_staged_record_is_not_available_until_verified() -> None:
    staged = _record()

    assert staged.status is ObjectStatus.STAGED
    assert staged.verified_at is None

    available = staged.verified(VERIFIED_AT)

    assert available.status is ObjectStatus.AVAILABLE
    assert available.verified_at == VERIFIED_AT
    assert staged.status is ObjectStatus.STAGED, "records are immutable"


def test_record_cannot_claim_available_without_a_verification_time() -> None:
    with pytest.raises(ValueError, match="AVAILABLE"):
        StorageObjectRecord(
            object_id=OBJECT_ID,
            status=ObjectStatus.AVAILABLE,
            storage_provider="LOCAL",
            bucket_name="b",
            object_key=KEY,
            provider_version_id=BODY_SHA256,
            content_hash=BODY_SHA256,
            byte_size=1,
            file_format="PARQUET",
            compression_codec=UNCOMPRESSED_CODEC,
            media_type=PARQUET_MEDIA_TYPE,
            schema_version="1.0.0",
            row_count=1,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            retention_policy_version=RETENTION_POLICY_VERSION,
            created_at=CREATED_AT,
            verified_at=None,
        )


def test_record_rejects_blank_not_null_columns_and_bad_periods() -> None:
    with pytest.raises(ValueError, match="bucket_name"):
        StorageObjectRecord.staged(
            object_id=OBJECT_ID,
            receipt=ObjectReceipt(
                storage_provider="LOCAL",
                bucket_name="",
                object_key=KEY,
                provider_version_id=BODY_SHA256,
                content_hash=BODY_SHA256,
                byte_size=1,
            ),
            schema_version="1.0.0",
            row_count=1,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            created_at=CREATED_AT,
        )
    with pytest.raises(ValueError, match="period_end"):
        StorageObjectRecord.staged(
            object_id=OBJECT_ID,
            receipt=_receipt(),
            schema_version="1.0.0",
            row_count=1,
            period_start=PERIOD_END,
            period_end=PERIOD_START,
            created_at=CREATED_AT,
        )
    with pytest.raises(ValueError, match="row_count"):
        StorageObjectRecord.staged(
            object_id=OBJECT_ID,
            receipt=_receipt(),
            schema_version="1.0.0",
            row_count=-1,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            created_at=CREATED_AT,
        )


def test_record_to_row_populates_every_canonical_storage_objects_column() -> None:
    row = _record().verified(VERIFIED_AT).to_row()

    assert row == StorageObjectRow(
        id=OBJECT_ID,
        status=ObjectStatus.AVAILABLE,
        storage_provider="S3_COMPATIBLE",
        bucket_name="idea2strategy-backtest",
        object_key=KEY,
        provider_version_id="version-1",
        content_hash=BODY_SHA256,
        byte_size=17,
        file_format="PARQUET",
        compression_codec="UNCOMPRESSED",
        media_type="application/vnd.apache.parquet",
        schema_version="1.0.0",
        row_count=3,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        encryption_key_ref=None,
        retention_policy_version=RETENTION_POLICY_VERSION,
        retention_until=None,
        legal_hold=False,
        created_at=CREATED_AT,
        verified_at=VERIFIED_AT,
        quarantined_at=None,
        superseded_at=None,
        deleted_at=None,
    )
    for column in (
        "status",
        "storage_provider",
        "bucket_name",
        "object_key",
        "provider_version_id",
        "content_hash",
        "byte_size",
        "file_format",
        "compression_codec",
        "media_type",
        "schema_version",
        "retention_policy_version",
        "created_at",
    ):
        assert getattr(row, column) is not None, f"{column} is NOT NULL in storage.objects"


def test_record_supersede_and_quarantine_are_terminal_state_changes() -> None:
    available = _record().verified(VERIFIED_AT)

    superseded = available.superseded(datetime(2025, 12, 1, tzinfo=UTC))
    quarantined = available.quarantined(datetime(2025, 12, 2, tzinfo=UTC))

    assert superseded.status is ObjectStatus.SUPERSEDED
    assert superseded.superseded_at == datetime(2025, 12, 1, tzinfo=UTC)
    assert quarantined.status is ObjectStatus.QUARANTINED
    assert quarantined.quarantined_at == datetime(2025, 12, 2, tzinfo=UTC)
    with pytest.raises(ValueError, match="verified"):
        _record().superseded(datetime(2025, 12, 1, tzinfo=UTC))


def test_storage_object_write_port_refuses_to_write_while_ownership_is_undecided() -> None:
    """`storage` is SHARED in DatabaseAccessPolicy but D-owned in the checklist.

    The port exists so the call site is written and typed; it fails closed rather than
    issuing an unauthorised INSERT.
    """

    port = UnauthorizedStorageObjectWritePort()

    with pytest.raises(StorageWriteNotAuthorized) as raised:
        port.register(_record())

    message = str(raised.value)
    assert "storage" in message
    assert "SHARED" in message

    # Every operation refuses, not just the insert: a caller cannot promote or read
    # back a row through a port that is not allowed to write one.
    for call in (
        lambda: port.mark_available(OBJECT_ID, VERIFIED_AT),
        lambda: port.quarantine(OBJECT_ID, VERIFIED_AT),
        lambda: port.find(OBJECT_ID),
    ):
        with pytest.raises(StorageWriteNotAuthorized, match="SHARED"):
            call()


# --------------------------------------------------------------------------------
# registration: exactly one row, AVAILABLE only after verification
# --------------------------------------------------------------------------------


def _publish(registrar: StorageObjectRegistrar, *, data: bytes = BODY, key: str = KEY) -> Any:
    return registrar.publish(
        object_id=OBJECT_ID,
        object_key=key,
        data=data,
        schema_version="1.0.0",
        row_count=3,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        created_at=CREATED_AT,
        verified_at=VERIFIED_AT,
    )


def test_registry_inserts_the_row_staged_and_promotes_the_same_row(tmp_path: Path) -> None:
    registry = InMemoryStorageObjectRegistry()
    seen: list[ObjectStatus] = []
    original = registry.register

    def spy(record: StorageObjectRecord) -> UUID:
        seen.append(record.status)
        return original(record)

    registry.register = spy  # type: ignore[method-assign]
    store = LocalObjectStore(tmp_path / "objects", bucket_name="b")

    published = _publish(StorageObjectRegistrar(store, registry))

    assert seen == [ObjectStatus.STAGED], "the row exists before the object is trusted"
    assert published.record.status is ObjectStatus.AVAILABLE
    assert registry.rows() == (published.record,)


def test_registrar_quarantines_the_row_when_the_object_is_tampered(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    registry = InMemoryStorageObjectRegistry()

    class _TamperingStore(LocalObjectStore):
        def put(self, object_key: str, data: bytes) -> ObjectReceipt:
            receipt = super().put(object_key, data)
            self.path_for(object_key).write_bytes(OTHER_BODY)
            return receipt

    with pytest.raises(ObjectVerificationError, match="quarantined"):
        _publish(StorageObjectRegistrar(_TamperingStore(root, bucket_name="b"), registry))

    rows = registry.rows()
    assert [row.status for row in rows] == [ObjectStatus.QUARANTINED]
    assert rows[0].verified_at is None, "a quarantined object was never AVAILABLE"
    assert rows[0].object_key == KEY, "the row is kept as evidence, not deleted"


def test_registrar_refuses_when_the_store_returns_different_bytes(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects", bucket_name="b")
    registry = InMemoryStorageObjectRegistry()
    registrar = StorageObjectRegistrar(store, registry)

    with pytest.raises(ObjectVerificationError, match="different object"):
        registrar.publish(
            object_id=OBJECT_ID,
            object_key=KEY,
            data=BODY,
            schema_version="1.0.0",
            row_count=3,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            created_at=CREATED_AT,
            verified_at=VERIFIED_AT,
            expected_content_hash=OTHER_SHA256,
        )

    assert registry.rows() == (), "no row for an object the caller did not ask for"


def test_registry_refuses_a_different_object_under_a_taken_identity() -> None:
    registry = InMemoryStorageObjectRegistry()
    registry.register(_record())

    other_key = BacktestObjectKey(
        run_id=RUN_ID,
        record_type="TRADE_DETAIL",
        week_start=WEEK_START,
        part_number=2,
        content_hash=OTHER_SHA256,
    ).render()
    conflicting = StorageObjectRecord.staged(
        object_id=OBJECT_ID,
        receipt=replace_receipt(object_key=other_key, content_hash=OTHER_SHA256),
        schema_version="1.0.0",
        row_count=3,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        created_at=CREATED_AT,
    )

    with pytest.raises(ObjectStoreConflict, match="different object"):
        registry.register(conflicting)
    assert len(registry.rows()) == 1


def test_registry_refuses_a_second_identity_for_the_same_object_key() -> None:
    registry = InMemoryStorageObjectRegistry()
    registry.register(_record())
    duplicate = StorageObjectRecord.staged(
        object_id=UUID("00000000-0000-4000-8000-0000000000c2"),
        receipt=_receipt(),
        schema_version="1.0.0",
        row_count=3,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        created_at=CREATED_AT,
    )

    with pytest.raises(ObjectStoreConflict, match="already registered"):
        registry.register(duplicate)
    assert len(registry.rows()) == 1


def test_registry_only_ever_inserts_a_staged_row_and_verifies_once() -> None:
    registry = InMemoryStorageObjectRegistry()

    with pytest.raises(ObjectStoreConflict, match="STAGED"):
        registry.register(_record().verified(VERIFIED_AT))
    assert registry.rows() == ()

    with pytest.raises(ObjectStoreConflict, match="not found"):
        registry.mark_available(OBJECT_ID, VERIFIED_AT)

    registry.register(_record())
    promoted = registry.mark_available(OBJECT_ID, VERIFIED_AT)
    assert promoted.status is ObjectStatus.AVAILABLE
    assert registry.mark_available(OBJECT_ID, VERIFIED_AT) == promoted
    with pytest.raises(ObjectStoreConflict, match="already verified"):
        registry.mark_available(OBJECT_ID, datetime(2025, 12, 9, tzinfo=UTC))


def test_registry_cleanup_fences_the_provider_version_and_is_idempotent() -> None:
    registry = InMemoryStorageObjectRegistry()
    record = _record().verified(VERIFIED_AT)
    registry.register(_record())
    registry.mark_available(record.object_id, VERIFIED_AT)

    with pytest.raises(
        ObjectStoreConflict, match="provider version"
    ), registry.cleanup_batch(
        (replace(record, provider_version_id="different-provider-version"),)
    ):
        pass
    assert registry.rows() == (record,)

    with registry.cleanup_batch((record,)) as locked:
        assert locked == (record,)
    assert registry.rows() == ()
    with registry.cleanup_batch((record,)) as locked:
        assert locked == ()


def test_registry_preflights_every_candidate_before_removing_any_row() -> None:
    registry = InMemoryStorageObjectRegistry()
    first = replace(
        _record(),
        object_id=UUID("00000000-0000-4000-8000-0000000000c2"),
        object_key=KEY.replace("part=0001", "part=0002"),
    )
    second = _record()
    registry.register(first)
    registry.register(second)

    with pytest.raises(
        ObjectStoreConflict, match="provider version"
    ), registry.cleanup_batch(
        (first, replace(second, provider_version_id="later-conflict"))
    ):
        pass

    assert registry.rows() == tuple(sorted((first, second), key=lambda item: item.object_key))


def replace_receipt(*, object_key: str, content_hash: str) -> ObjectReceipt:
    return ObjectReceipt(
        storage_provider="S3_COMPATIBLE",
        bucket_name="idea2strategy-backtest",
        object_key=object_key,
        provider_version_id="version-2",
        content_hash=content_hash,
        byte_size=len(OTHER_BODY),
    )


# --------------------------------------------------------------------------------
# Docker: the same contract against a real S3 emulator
# --------------------------------------------------------------------------------


LOCALSTACK_IMAGE = "localstack/localstack:4.7.0"


def _docker_is_available() -> bool:
    try:
        import docker
    except ImportError:  # pragma: no cover - testcontainers depends on docker
        return False
    try:
        docker.from_env().ping()
    except Exception:  # pragma: no cover - depends on the developer's machine
        return False
    else:
        return True


@pytest.fixture(scope="module")
def localstack_s3() -> Iterator[Any]:
    if not _docker_is_available():  # pragma: no cover - environment dependent
        pytest.skip(
            "missing dependency: a reachable Docker daemon for the "
            f"{LOCALSTACK_IMAGE} S3 emulator. The S3 leg of the object-store contract "
            "is NOT covered by this run; the default run deselects `-m docker`."
        )

    from testcontainers.community.localstack import LocalStackContainer

    # us-east-1: any other region makes CreateBucket require a LocationConstraint.
    with LocalStackContainer(image=LOCALSTACK_IMAGE, region_name="us-east-1").with_services("s3") as container:
        yield container.get_client("s3")


@pytest.fixture
def emulated_store(localstack_s3: Any, request: pytest.FixtureRequest) -> S3ObjectStore:
    bucket = f"bt-{abs(hash(request.node.name)) % 10**12}"
    localstack_s3.create_bucket(Bucket=bucket)
    return S3ObjectStore(bucket, client=localstack_s3, sleep=_no_sleep)


@pytest.mark.docker
@pytest.mark.parametrize("contract", CONTRACT, ids=lambda item: item.__name__)
def test_object_store_contract_against_a_real_s3_emulator(
    contract: Callable[[ObjectStore], None], emulated_store: S3ObjectStore
) -> None:
    contract(emulated_store)


@pytest.mark.docker
def test_real_emulator_enforces_the_if_none_match_precondition(emulated_store: S3ObjectStore) -> None:
    """The immutability guarantee is only real if the server enforces `If-None-Match: *`.

    If this fails, `S3ObjectStore.put` degrades to last-writer-wins on a genuine race and
    the emulator cannot witness the 412 path. It is asserted, not assumed.
    """

    emulated_store.put(KEY, BODY)

    with pytest.raises(ClientError) as raised:
        emulated_store.client.put_object(
            Bucket=emulated_store.bucket,
            Key=KEY,
            Body=OTHER_BODY,
            IfNoneMatch="*",
        )

    assert raised.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert emulated_store.open(KEY).read() == BODY


@pytest.mark.docker
@pytest.mark.parametrize("newer_body", [BODY, OTHER_BODY], ids=["identical-hash", "changed"])
def test_real_emulator_compensation_deletes_only_the_registered_provider_version(
    emulated_store: S3ObjectStore,
    newer_body: bytes,
) -> None:
    emulated_store.client.put_bucket_versioning(
        Bucket=emulated_store.bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    registered = emulated_store.put(KEY, BODY)
    newer_hash = hashlib.sha256(newer_body).hexdigest()
    newer = emulated_store.client.put_object(
        Bucket=emulated_store.bucket,
        Key=KEY,
        Body=newer_body,
        Metadata={"sha256": newer_hash},
        ContentType=PARQUET_MEDIA_TYPE,
    )

    assert registered.provider_version_id
    assert newer["VersionId"] != registered.provider_version_id
    assert emulated_store.preflight_delete(
        KEY,
        BODY_SHA256,
        registered.provider_version_id,
    ) is True
    assert emulated_store.delete_if_matches(
        KEY,
        BODY_SHA256,
        registered.provider_version_id,
    ) is True

    current = emulated_store.client.get_object(Bucket=emulated_store.bucket, Key=KEY)
    assert current["VersionId"] == newer["VersionId"]
    assert current["Body"].read() == newer_body
    with pytest.raises(ClientError) as missing_registered_version:
        emulated_store.client.head_object(
            Bucket=emulated_store.bucket,
            Key=KEY,
            VersionId=registered.provider_version_id,
        )
    assert (
        missing_registered_version.value.response["ResponseMetadata"]["HTTPStatusCode"]
        == 404
    )


@pytest.mark.docker
def test_real_emulator_412_race_is_reconciled_not_overwritten(emulated_store: S3ObjectStore) -> None:
    """Another writer wins between our HEAD and our PUT; we must reconcile."""

    seen: list[str] = []
    original_head = emulated_store.client.head_object

    def head_then_let_the_racer_win(**kwargs: Any) -> dict[str, Any]:
        if not seen:
            seen.append("first")
            emulated_store.client.put_object(
                Bucket=emulated_store.bucket,
                Key=kwargs["Key"],
                Body=BODY,
                Metadata={"sha256": BODY_SHA256},
                ContentType=PARQUET_MEDIA_TYPE,
            )
            raise _client_error("404", 404, "HeadObject")
        return original_head(**kwargs)

    emulated_store.client.head_object = head_then_let_the_racer_win  # type: ignore[method-assign]
    try:
        receipt = emulated_store.put(KEY, BODY)
    finally:
        emulated_store.client.head_object = original_head  # type: ignore[method-assign]

    assert receipt.reconciled is True
    assert receipt.content_hash == BODY_SHA256
    assert emulated_store.open(KEY).read() == BODY
