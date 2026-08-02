# backtest contribution fixtures

Test-only material. The central Flyway assembler must never scan this directory as a
migration source, and nothing here has a bundle-eligible `V<digits>__` filename.

## `central-migration/`

A byte-for-byte copy of the applied central Flyway bundle
(`backend/db-migration/src/main/resources/db/migration/`), carrying the project's
`.sql.fixture` suffix so that no `*.sql` glob — least of all the central assembler —
can mistake it for a contributed migration. Only the filename differs; the bytes are
identical, and the recorded digests are digests of the original content.

The Testcontainers integration suite applies **this** SQL rather than hand-written
DDL, so the persistence layer is proven against the canonical schema and not against a
convenient restatement of it.

Why a vendored copy instead of reading the superproject directly: this repository is a
Git submodule and its own CI checks out only this repository, where
`backend/db-migration` does not exist. A copy keeps the integration suite runnable
from a bare clone and keeps the applied SQL identical in CI and on a developer
machine.

The copy is guarded two ways by `tests/persistence/test_central_migration_fixture.py`:

1. Its digests must match `central-migration.sha256` — a local edit fails the suite.
2. When a superproject checkout happens to be reachable (developer machine, root
   integration CI), the copy must still match the central files byte-for-byte. If A
   changes the central bundle, this test fails and the copy must be refreshed in a
   reviewed change.

Refresh procedure: copy the files again, regenerate `central-migration.sha256` with
`sha256sum`, and state in the PR which central commit was copied.

## `backtest_reference_seed.sql.fixture`

Minimal upstream rows the `backtest.*` foreign keys require (`identity.accounts`,
`bot.bots`, the two `trading` policy versions, the `market_data` provider/feed/
instrument/manifest/feature chain, one `storage.objects` row). This is review-grade
sample data for integration tests only. It is not a seed, not a migration, and must
never be applied to a real database.
