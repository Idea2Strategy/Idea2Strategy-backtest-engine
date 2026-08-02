"""Reader for this repository's COM07 migration-contribution root.

The central Flyway assembler in `backend/db-migration` is the authority. This module
only reads `db/migration-contributions/contribution.properties` and re-states the
central facts this repository depends on, so that a bare clone can verify its own
half of the contract without a superproject checkout.

Nothing here executes SQL or applies migrations: `runtime.flyway.enabled` must stay
`false` and the runtime never owns migration execution.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


__all__ = [
    "CENTRAL_MIGRATION_OWNERS",
    "CENTRAL_SCHEMA_OWNERS",
    "RUNTIME_ROW_ONLY_SCHEMAS",
    "ContributionError",
    "MigrationContribution",
    "default_contribution_root",
    "load_contribution",
    "superproject_root",
]


# Mirrors `MigrationOwner` in
# backend/db-migration/src/main/java/com/idea2strategy/backend/migration/MigrationOwner.java
CENTRAL_MIGRATION_OWNERS: tuple[str, ...] = ("backend", "trading", "backtest", "pipeline", "shared")

# Mirrors `DatabaseAccessPolicy.SCHEMA_OWNERS` in the same package. `storage` is
# registered SHARED there even though the implementation checklist calls it D-owned;
# that contradiction is unresolved, so this repository authors no `storage` DDL.
CENTRAL_SCHEMA_OWNERS: dict[str, str] = {
    "identity": "backend",
    "strategy": "backend",
    "bot": "backend",
    "storage": "shared",
    "market_data": "pipeline",
    "trading": "trading",
    "backtest": "backtest",
    "performance": "backend",
    "competition": "backend",
    "operations": "backend",
}

#: Schemas this repository writes rows to but authors no DDL for.
#:
#: `storage` only. Spec rule 2 lists it among D's writable schemas and spec 2.5 makes
#: `storage.objects` registration mandatory, but spec 2.4 forbids D from adding
#: `storage` DDL while the central ownership registration says SHARED. Row writes and
#: schema changes are separate permissions and this constant is where they diverge.
RUNTIME_ROW_ONLY_SCHEMAS: frozenset[str] = frozenset({"storage"})

_REQUIRED_KEYS = (
    "contract.version",
    "owner",
    "schemas",
    "migrations.directory",
    "fixtures.directory",
    "filename.regex",
    "runtime.flyway.enabled",
)

_SUPERPROJECT_ENV = "I2S_SUPERPROJECT_ROOT"
_SUPERPROJECT_SEARCH_DEPTH = 6


class ContributionError(ValueError):
    """Raised when the contribution root violates the central COM07 contract."""


@dataclass(frozen=True, slots=True)
class MigrationContribution:
    """Parsed `contribution.properties` for one owner repository."""

    root: Path
    contract_version: int
    owner: str
    schemas: tuple[str, ...]
    migrations_directory: Path
    fixtures_directory: Path
    filename_regex: str
    runtime_flyway_enabled: bool

    def writable_schemas(self) -> frozenset[str]:
        """Schemas this repository may write **rows** to at runtime.

        Two different questions hide behind the word "own", and conflating them is
        what made the `storage` schema look contradictory:

        * *Who authors migrations for it?* That is `self.schemas`, the COM07
          declaration the central Flyway assembler reads. `storage` is **not** in it,
          and must not be: spec 2.4 records that `DatabaseAccessPolicy` registers
          `storage` as SHARED, so this repository authors no `storage` DDL.
        * *Who writes rows to it at runtime?* That additionally includes
          `RUNTIME_ROW_ONLY_SCHEMAS`. Spec rule 2 puts `storage` among the schemas D
          writes, and spec 2.5 requires every stored object to register a row in
          `storage.objects` before it can become `AVAILABLE`.

        The DDL guard is unaffected either way: `engine.check_statement` rejects DDL
        against every schema, including these.
        """

        return frozenset(self.schemas) | RUNTIME_ROW_ONLY_SCHEMAS


def default_contribution_root() -> Path:
    """`<repo>/db/migration-contributions`, resolved from this module's location."""

    return Path(__file__).resolve().parents[3] / "db" / "migration-contributions"


def superproject_root() -> Path | None:
    """Best-effort location of the Idea2Strategy superproject checkout, if reachable.

    Returns `None` from a bare clone of this repository, which is the normal case in
    this repository's own CI. Callers must treat a `None` result as "cannot
    cross-check", never as "the central source agrees".
    """

    override = os.environ.get(_SUPERPROJECT_ENV)
    if override:
        candidate = Path(override).expanduser().resolve()
        return candidate if (candidate / "db" / "schema.dbml").is_file() else None

    here = Path(__file__).resolve()
    ancestors = list(here.parents)[:_SUPERPROJECT_SEARCH_DEPTH]
    for parent in ancestors:
        if _is_superproject(parent):
            return parent
    # Git worktrees live beside the superproject rather than inside it.
    for parent in ancestors:
        try:
            siblings = sorted(child for child in parent.iterdir() if child.is_dir())
        except OSError:
            continue
        for sibling in siblings:
            if _is_superproject(sibling):
                return sibling
    return None


def _is_superproject(candidate: Path) -> bool:
    return (candidate / "db" / "schema.dbml").is_file() and (candidate / "backend").is_dir()


def load_contribution(root: Path | None = None) -> MigrationContribution:
    """Parse and validate `contribution.properties` under `root`."""

    root = (root or default_contribution_root()).resolve()
    properties_path = root / "contribution.properties"
    if not properties_path.is_file():
        raise ContributionError(f"contribution.properties is missing: {properties_path}")

    values = _parse_properties(properties_path)
    missing = [key for key in _REQUIRED_KEYS if key not in values]
    if missing:
        raise ContributionError(f"contribution.properties is missing keys: {', '.join(missing)}")

    contract_version = _parse_int(values["contract.version"], "contract.version")
    if contract_version != 1:
        raise ContributionError(f"unsupported contract.version: {contract_version}")

    owner = values["owner"].strip()
    if owner not in CENTRAL_MIGRATION_OWNERS:
        raise ContributionError(f"owner is not a central MigrationOwner token: {owner!r}")

    schemas = tuple(part.strip() for part in values["schemas"].split(",") if part.strip())
    if not schemas:
        raise ContributionError("schemas must declare at least one schema")
    for schema in schemas:
        registered = CENTRAL_SCHEMA_OWNERS.get(schema)
        if registered is None:
            raise ContributionError(f"schema is not registered centrally: {schema!r}")
        if registered != owner:
            raise ContributionError(f"schema {schema!r} is registered to owner {registered!r}, not {owner!r}")

    runtime_flyway = _parse_bool(values["runtime.flyway.enabled"], "runtime.flyway.enabled")
    if runtime_flyway:
        raise ContributionError(
            "runtime.flyway.enabled must be false; migration execution belongs to the central bundle"
        )

    migrations_directory = _resolve_inside(root, values["migrations.directory"], "migrations.directory")
    fixtures_directory = _resolve_inside(root, values["fixtures.directory"], "fixtures.directory")

    filename_regex = values["filename.regex"].strip()
    if not filename_regex:
        raise ContributionError("filename.regex must not be empty")
    if f"__{owner}_" not in filename_regex:
        raise ContributionError("filename.regex must pin the owner token in the filename")

    return MigrationContribution(
        root=root,
        contract_version=contract_version,
        owner=owner,
        schemas=schemas,
        migrations_directory=migrations_directory,
        fixtures_directory=fixtures_directory,
        filename_regex=filename_regex,
        runtime_flyway_enabled=runtime_flyway,
    )


def _parse_properties(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith(("#", "!")):
            continue
        if "=" not in line:
            raise ContributionError(f"{path.name}:{lineno} is not a key=value line")
        key, _, value = line.partition("=")
        key = key.strip()
        if key in values:
            raise ContributionError(f"{path.name}:{lineno} redefines key {key!r}")
        values[key] = value.strip()
    return values


def _parse_int(value: str, key: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ContributionError(f"{key} must be an integer, got {value!r}") from exc


def _parse_bool(value: str, key: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    raise ContributionError(f"{key} must be 'true' or 'false', got {value!r}")


def _resolve_inside(root: Path, declared: str, key: str) -> Path:
    if not declared.strip():
        raise ContributionError(f"{key} must not be empty")
    candidate = Path(declared)
    if candidate.is_absolute():
        raise ContributionError(f"{key} must be relative to the contribution root")
    resolved = (root / candidate).resolve()
    if resolved != root and root not in resolved.parents:
        raise ContributionError(f"{key} resolves outside the contribution root: {declared!r}")
    if not resolved.is_dir():
        raise ContributionError(f"{key} does not exist: {resolved}")
    return resolved
