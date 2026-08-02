"""`storage.objects` registration against a real PostgreSQL 16 container.

Spec 2.5: every stored object holds exactly one row here, and reaches `AVAILABLE`
**only after verification**. These tests exercise that ordering as a structural
property of the repository, not as a convention the caller is trusted to follow.

Spec 2.4 draws the other boundary: this repository writes `storage` *rows* but authors
no `storage` DDL. `test_runtime_no_ddl.py` covers the DDL half.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backtest_engine.persistence import (
    BacktestPersistence,
    InvalidStatusTransition,
    ObjectStatus,
    PublishConflict,
    RowNotFound,
)

from .support import make_storage_object


pytestmark = pytest.mark.docker


VERIFIED_AT = datetime(2026, 3, 2, 14, 6, tzinfo=UTC)
QUARANTINED_AT = datetime(2026, 3, 2, 14, 7, tzinfo=UTC)


def test_register_inserts_a_staged_object(persistence: BacktestPersistence) -> None:
    row = make_storage_object()

    with persistence.unit_of_work() as uow:
        stored, inserted = uow.objects.register(row)

    assert inserted is True
    assert stored.id == row.id
    assert stored.status is ObjectStatus.STAGED
    assert stored.verified_at is None
    assert stored.object_key == row.object_key


def test_register_is_idempotent_for_the_same_object(persistence: BacktestPersistence) -> None:
    """An upload retried after a lost response must not be a second row or an error."""
    row = make_storage_object()

    with persistence.unit_of_work() as uow:
        first, first_inserted = uow.objects.register(row)
    with persistence.unit_of_work() as uow:
        second, second_inserted = uow.objects.register(row)

    assert first_inserted is True
    assert second_inserted is False
    assert first == second


def test_reusing_an_object_id_for_different_bytes_is_a_conflict(
    persistence: BacktestPersistence,
) -> None:
    """The object id is what a detail manifest points at; it cannot be repointed."""
    row = make_storage_object()
    with persistence.unit_of_work() as uow:
        uow.objects.register(row)

    impostor = make_storage_object(object_id=row.id, content_hash="c" * 64)

    with pytest.raises(PublishConflict, match="content_hash"), persistence.unit_of_work() as uow:
        uow.objects.register(impostor)


@pytest.mark.parametrize(
    "status",
    [ObjectStatus.AVAILABLE, ObjectStatus.QUARANTINED, ObjectStatus.SUPERSEDED, ObjectStatus.DELETED],
)
def test_an_object_cannot_be_registered_already_published(
    persistence: BacktestPersistence, status: ObjectStatus
) -> None:
    """`AVAILABLE` is reachable only through `mark_available`, never on insert.

    Without this, "verified before available" is a comment rather than a rule: a caller
    could insert an AVAILABLE row and never verify anything.
    """
    row = make_storage_object(status=status)

    with pytest.raises(PublishConflict, match="STAGED"), persistence.unit_of_work() as uow:
        uow.objects.register(row)


def test_an_object_cannot_be_registered_pre_verified(persistence: BacktestPersistence) -> None:
    row = make_storage_object(verified_at=VERIFIED_AT)

    with pytest.raises(PublishConflict, match="verified"), persistence.unit_of_work() as uow:
        uow.objects.register(row)


def test_mark_available_publishes_a_verified_object(persistence: BacktestPersistence) -> None:
    row = make_storage_object()
    with persistence.unit_of_work() as uow:
        uow.objects.register(row)

    with persistence.unit_of_work() as uow:
        published = uow.objects.mark_available(row.id, VERIFIED_AT)

    assert published.status is ObjectStatus.AVAILABLE
    assert published.verified_at == VERIFIED_AT

    with persistence.read_only() as uow:
        assert uow.objects.require_available([row.id])[0].id == row.id


def test_mark_available_is_idempotent(persistence: BacktestPersistence) -> None:
    row = make_storage_object()
    with persistence.unit_of_work() as uow:
        uow.objects.register(row)
        first = uow.objects.mark_available(row.id, VERIFIED_AT)

    with persistence.unit_of_work() as uow:
        second = uow.objects.mark_available(row.id, datetime(2026, 3, 2, 15, 0, tzinfo=UTC))

    assert first == second
    assert second.verified_at == VERIFIED_AT


def test_a_quarantined_object_cannot_become_available(persistence: BacktestPersistence) -> None:
    """Corruption is not undone by retrying the publish step."""
    row = make_storage_object()
    with persistence.unit_of_work() as uow:
        uow.objects.register(row)
        uow.objects.quarantine(row.id, QUARANTINED_AT)

    with pytest.raises(InvalidStatusTransition, match="QUARANTINED"), persistence.unit_of_work() as uow:
        uow.objects.mark_available(row.id, VERIFIED_AT)


def test_an_object_published_then_found_corrupt_can_be_quarantined(
    persistence: BacktestPersistence,
) -> None:
    row = make_storage_object()
    with persistence.unit_of_work() as uow:
        uow.objects.register(row)
        uow.objects.mark_available(row.id, VERIFIED_AT)

    with persistence.unit_of_work() as uow:
        quarantined = uow.objects.quarantine(row.id, QUARANTINED_AT)

    assert quarantined.status is ObjectStatus.QUARANTINED
    assert quarantined.quarantined_at == QUARANTINED_AT
    with pytest.raises(RowNotFound, match="AVAILABLE"), persistence.read_only() as uow:
        uow.objects.require_available([row.id])


def test_marking_an_unknown_object_available_is_not_found(
    persistence: BacktestPersistence,
) -> None:
    with pytest.raises(RowNotFound), persistence.unit_of_work() as uow:
        uow.objects.mark_available(uuid4(), VERIFIED_AT)


def test_find_returns_none_for_an_unknown_object(persistence: BacktestPersistence) -> None:
    with persistence.read_only() as uow:
        assert uow.objects.find(uuid4()) is None


def test_a_failed_registration_leaves_no_row_behind(persistence: BacktestPersistence) -> None:
    """The unit of work is one transaction; a rejected publish is not half-applied."""
    good = make_storage_object()
    bad = make_storage_object(status=ObjectStatus.AVAILABLE)

    with pytest.raises(PublishConflict), persistence.unit_of_work() as uow:
        uow.objects.register(good)
        uow.objects.register(bad)

    with persistence.read_only() as uow:
        assert uow.objects.find(good.id) is None
