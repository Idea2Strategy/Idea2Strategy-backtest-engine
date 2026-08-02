# Backtest migration contributions

This directory is the machine-readable handoff boundary from `backtest-engine` to the
central Flyway bundle assembled by `backend/db-migration` (owner A). It is the
COM07 mechanism only — **this repository never executes Flyway and never applies DDL
at runtime**.

## contribution.properties

| key | value | meaning |
|---|---|---|
| `contract.version` | `1` | contribution format version |
| `owner` | `backtest` | must equal the owner token in every contributed filename; one of the tokens in the central `MigrationOwner` enum |
| `schemas` | `backtest` | schemas this repository may mutate; central `DatabaseAccessPolicy` remains authoritative at table level |
| `migrations.directory` | `migrations` | the only directory that may contribute production Flyway SQL |
| `fixtures.directory` | `fixtures` | test-only material the central bundle must never scan |
| `filename.regex` | `^V[0-9]{14}__backtest_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$` | complete filename contract |
| `runtime.flyway.enabled` | `false` | migration execution belongs to the central one-shot deployment step |

`V001`-style legacy numbering is rejected centrally and by
`tests/persistence/test_migration_contribution.py`. New files use
`V<YYYYMMDDHHMMSS>__backtest_<slug>.sql` with a UTC, globally unique timestamp.

## Current contents

`migrations/` holds one contributed migration:

| file | adds | why |
|---|---|---|
| `V20260802094500__backtest_run_input_pins.sql` | `backtest.run_input_pins` | Four of the eight values `GET /api/v1/backtests/{id}/inputs` reports are *arguments* of the SHA-256 in `runs.configuration_hash` and exist in no column. A digest cannot be inverted, so the read model had to invent them, report null, or refuse the route. |

It adds a table; it does not touch one the applied baseline created. All nine original
`backtest.*` tables remain exactly as `V1__initial_schema.sql` defines them, because
applied migrations are immutable.

The matching canonical-model change request is
[`change-requests/2026-08-02-backtest-run-input-pins.md`](change-requests/2026-08-02-backtest-run-input-pins.md).
The root `db/schema.dbml` lives in the superproject and is owned centrally, so this
repository proposes the change rather than making it. **It is proposed, not approved.**

`change-requests/` holds those proposals. It is documentation: the central assembler
never scans it, and nothing there is SQL.

`fixtures/` holds the vendored copy of the central migration bundle that the
Testcontainers integration tests apply, plus its recorded SHA-256 digests. It contains
no bundle-eligible filenames. `tests/conftest.py` applies the vendored bundle **and
then** `migrations/`, in the order the central assembler would, so the integration
suite runs against the schema the deployment will have.

## Open item: `storage` schema ownership is contradictory

`docs/backend-implementation-master-checklist.md` treats `storage` as D-owned, but
`backend/db-migration/.../DatabaseAccessPolicy.java:36` registers it as
`MigrationOwner.SHARED`. The two disagree and the disagreement is **not** resolved
here.

Consequences, deliberately chosen:

- `schemas=backtest` only. This contribution does **not** claim `storage`.
- No `storage` DDL is authored in this repository, now or as part of this rebuild.
- The backtest persistence layer treats `storage.objects` as **read-only**
  (`StorageObjectReader`), even though `DatabaseAccessPolicy.allowsBacktest` would
  permit `INSERT`. Writing rows for backtest result objects is deferred to BT3, which
  must first get the ownership question answered by owner A.

This needs a decision from the central migration owner before any repository writes
`storage`. Until then, treat any `storage` write from this repository as a contract
violation.

## Related caveat: the access policy is not enforced at runtime

`DatabaseAccessPolicy` is a static helper used by central unit tests. The applied
baseline contains no `GRANT`/`REVOKE` statements and no per-service roles, so nothing
in the live database prevents a service from writing a schema it does not own — the
backend already inserts into `backtest.runs` directly. This repository compensates
only for itself: `backtest_engine.persistence` restricts its own statements to the
declared schemas and refuses DDL, and `tests/persistence/test_runtime_no_ddl.py`
proves it.
