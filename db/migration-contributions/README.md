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

All nine `backtest.*` tables already exist in the applied central baseline
`V1__initial_schema.sql` (`CREATE TABLE "backtest"."runs"` and following). Applied
migrations are immutable, so this repository never restates them; it contributes only
later changes.

`migrations/` contains one file:

- `V20260802143000__backtest_run_outcome_detail.sql` — adds three nullable columns to
  `backtest.runs`: `result_manifest_id` (the COMPLETED event's `resultManifestId`),
  `retryable` (the FAILED event's `retryable`) and `missing_requirements` (the
  UNAVAILABLE event's `missingRequirements`). All three are fields the
  `backtest.v1` result contract already declares **required** in their branch and
  that the server previously validated and then discarded, because the baseline had
  nowhere to put them.

**Canonical model.** The root superproject's `db/schema.dbml` is authoritative and is
not part of this repository, so it cannot be edited from here. The matching DBML
change — three columns on `Table backtest.runs` — is stated verbatim in
`docs/dbml-change-request-run-outcome-detail.md` and must land in the same reviewed
change as this migration.

`fixtures/` holds the vendored copy of the central migration bundle that the
Testcontainers integration tests apply, plus its recorded SHA-256 digests. It contains
no bundle-eligible filenames.

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
