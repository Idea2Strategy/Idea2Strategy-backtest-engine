"""COM07 contribution-root contract for this repository.

These assertions are the local half of the central Flyway gate in
`backend/db-migration`. They must hold from a bare clone of this repository, so the
central facts (owner tokens, schema owners, baseline bytes) are vendored here and
cross-checked against the superproject whenever the superproject happens to be
checked out around us.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backtest_engine.persistence.contribution import (
    CENTRAL_MIGRATION_OWNERS,
    CENTRAL_SCHEMA_OWNERS,
    ContributionError,
    load_contribution,
    superproject_root,
)


CONTRIBUTION_ROOT = Path(__file__).resolve().parents[2] / "db" / "migration-contributions"


def test_contribution_properties_parse() -> None:
    contribution = load_contribution(CONTRIBUTION_ROOT)

    assert contribution.contract_version == 1
    assert contribution.owner == "backtest"
    assert contribution.schemas == ("backtest",)
    assert contribution.migrations_directory == CONTRIBUTION_ROOT / "migrations"
    assert contribution.fixtures_directory == CONTRIBUTION_ROOT / "fixtures"
    assert contribution.runtime_flyway_enabled is False


def test_owner_token_is_known_to_the_central_migration_owner_enum() -> None:
    contribution = load_contribution(CONTRIBUTION_ROOT)

    assert contribution.owner in CENTRAL_MIGRATION_OWNERS


def test_declared_schemas_are_writable_by_this_repository() -> None:
    contribution = load_contribution(CONTRIBUTION_ROOT)

    for schema in contribution.schemas:
        assert CENTRAL_SCHEMA_OWNERS[schema] == contribution.owner, (
            f"schema {schema!r} is registered to {CENTRAL_SCHEMA_OWNERS.get(schema)!r} centrally"
        )


def test_storage_schema_is_not_claimed_by_this_contribution() -> None:
    """`DatabaseAccessPolicy` registers `storage` as SHARED; D must not claim it."""

    contribution = load_contribution(CONTRIBUTION_ROOT)

    assert "storage" not in contribution.schemas
    assert CENTRAL_SCHEMA_OWNERS["storage"] == "shared"


def test_every_contributed_migration_matches_the_filename_regex() -> None:
    contribution = load_contribution(CONTRIBUTION_ROOT)
    pattern = re.compile(contribution.filename_regex)

    offenders = [
        path.name
        for path in sorted(contribution.migrations_directory.iterdir())
        if path.is_file() and path.suffix == ".sql" and not pattern.match(path.name)
    ]

    assert offenders == []


def test_filename_regex_rejects_legacy_numbering_and_foreign_owners() -> None:
    contribution = load_contribution(CONTRIBUTION_ROOT)
    pattern = re.compile(contribution.filename_regex)

    assert pattern.match("V20260802101500__backtest_add_run_dispatch_state.sql")
    assert not pattern.match("V001__backtest_add_run_dispatch_state.sql")
    assert not pattern.match("V20260802101500__trading_add_run_dispatch_state.sql")
    assert not pattern.match("V2026080210150__backtest_add_run.sql")
    assert not pattern.match("V20260802101500__backtest_Add_Run.sql")


def test_fixtures_directory_never_holds_bundle_eligible_sql() -> None:
    """Fixtures use the project's `.sql.fixture` suffix so no `*.sql` glob can find them."""

    contribution = load_contribution(CONTRIBUTION_ROOT)

    assert sorted(path.name for path in contribution.fixtures_directory.rglob("*.sql")) == []


def test_declared_directories_stay_inside_the_contribution_root(tmp_path: Path) -> None:
    (tmp_path / "contribution.properties").write_text(
        "contract.version=1\n"
        "owner=backtest\n"
        "schemas=backtest\n"
        "migrations.directory=../../escape\n"
        "fixtures.directory=fixtures\n"
        "filename.regex=^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$\n"
        "runtime.flyway.enabled=false\n",
        encoding="utf-8",
    )

    with pytest.raises(ContributionError, match="outside the contribution root"):
        load_contribution(tmp_path)


def test_runtime_flyway_must_stay_disabled(tmp_path: Path) -> None:
    (tmp_path / "contribution.properties").write_text(
        "contract.version=1\n"
        "owner=backtest\n"
        "schemas=backtest\n"
        "migrations.directory=migrations\n"
        "fixtures.directory=fixtures\n"
        "filename.regex=^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$\n"
        "runtime.flyway.enabled=true\n",
        encoding="utf-8",
    )
    (tmp_path / "migrations").mkdir()
    (tmp_path / "fixtures").mkdir()

    with pytest.raises(ContributionError, match="runtime.flyway.enabled"):
        load_contribution(tmp_path)


def test_vendored_central_facts_still_match_the_superproject() -> None:
    root = superproject_root()
    if root is None:
        pytest.skip("superproject checkout is not reachable from this worktree")

    owner_enum = root / ("backend/db-migration/src/main/java/com/idea2strategy/backend/migration/MigrationOwner.java")
    policy = root / ("backend/db-migration/src/main/java/com/idea2strategy/backend/migration/DatabaseAccessPolicy.java")
    if not owner_enum.is_file() or not policy.is_file():
        pytest.skip("central db-migration sources are not present in the superproject checkout")

    declared_owners = set(re.findall(r'^\s+[A-Z]+\("([a-z]+)"\)', owner_enum.read_text(encoding="utf-8"), re.M))
    assert declared_owners == set(CENTRAL_MIGRATION_OWNERS)

    policy_text = policy.read_text(encoding="utf-8")
    block = policy_text.split("SCHEMA_OWNERS = Map.of(", 1)[1].split(");", 1)[0]
    declared_schema_owners = {
        schema: owner.lower() for schema, owner in re.findall(r'"([a-z_]+)",\s*MigrationOwner\.([A-Z]+)', block)
    }
    assert declared_schema_owners == dict(CENTRAL_SCHEMA_OWNERS)
