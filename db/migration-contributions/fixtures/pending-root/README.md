# Pending root migration fixture

This fixture mirrors the provider-owned `run_input_pins` schema currently observed
in the Backend migration history. It exists only so this consumer repository can test
its SQLAlchemy mapping and PostgreSQL behavior against the proposed integration
target.

It is not a Flyway contribution, is not approved canonical source, and must not be
deployed from this repository. The historical
`V20260802094500__backtest_run_input_pins.sql` contribution is preserved byte-for-byte
under `../superseded-proposals/` and is excluded from current test assembly because its consumer-owned singular
dataset/feature shape was superseded by the normalized provider-owned bundle design.
