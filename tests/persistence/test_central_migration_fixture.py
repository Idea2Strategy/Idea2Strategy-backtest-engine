"""The vendored central bundle must stay identical to the central one.

Two independent guards, because they fail for different reasons:

* the recorded digests catch a local edit to the copy;
* the superproject comparison catches the central bundle moving underneath us.

The copy carries the project's `.sql.fixture` suffix so no `*.sql` glob — least of all
the central Flyway assembler — can mistake it for a contributed migration. The bytes
are unchanged; only the filename differs.
"""

from __future__ import annotations

import pytest

from backtest_engine.persistence.contribution import superproject_root
from conftest import VENDORED_MIGRATIONS, migration_files, recorded_digests, sha256_of


CENTRAL_RELATIVE = "backend/db-migration/src/main/resources/db/migration"
FIXTURE_SUFFIX = ".fixture"


def test_vendored_copy_matches_its_recorded_digests() -> None:
    recorded = recorded_digests()
    actual = {path.name: sha256_of(path) for path in migration_files()}

    assert actual == recorded


def test_vendored_bundle_is_ordered_by_flyway_version() -> None:
    names = [path.name for path in migration_files()]

    assert names == [
        "V1__initial_schema.sql.fixture",
        "V20260801112341__backend_identity_email_auth.sql.fixture",
        "V20260801153000__backend_bot_continuation_deadlines.sql.fixture",
    ]


def test_vendored_copy_matches_the_central_bundle() -> None:
    root = superproject_root()
    if root is None:
        pytest.skip("superproject checkout is not reachable from this worktree")
    central = root / CENTRAL_RELATIVE
    if not central.is_dir():
        pytest.skip("central migration directory is not present in the superproject checkout")

    central_names = sorted(path.name for path in central.glob("V*.sql"))
    vendored_names = sorted(
        path.name.removesuffix(FIXTURE_SUFFIX) for path in VENDORED_MIGRATIONS.glob("V*.sql.fixture")
    )
    assert vendored_names == central_names, (
        "the central Flyway bundle gained or lost a file; refresh "
        "db/migration-contributions/fixtures/central-migration/ in a reviewed change"
    )

    differing = [
        name
        for name in central_names
        if sha256_of(central / name) != sha256_of(VENDORED_MIGRATIONS / (name + FIXTURE_SUFFIX))
    ]
    assert differing == [], (
        f"central migration files changed: {differing}. Refresh the vendored copy and "
        "central-migration.sha256, and re-run the integration suite"
    )
