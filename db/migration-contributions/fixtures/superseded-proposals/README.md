# Superseded migration proposals

Files in this directory preserve historical SQL proposals for audit. Their
`.sql.fixture` suffix keeps them outside the central Flyway contribution glob and
they must never be deployed from this repository.

`V20260802143000__backtest_run_outcome_detail.sql.fixture` was superseded only
because its version is older than central migrations that have already landed. Its
three-column meaning is carried forward unchanged by the active, globally ordered
`V20260805170000__backtest_run_outcome_detail.sql` contribution.
