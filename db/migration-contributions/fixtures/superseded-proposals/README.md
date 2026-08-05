# Superseded migration proposals

Files in this directory preserve historical SQL proposals for audit. Their
`.sql.fixture` suffix keeps them outside the central Flyway contribution glob and
they must never be deployed from this repository.

`V20260802143000__backtest_run_outcome_detail.sql.fixture` was superseded only
because its version is older than central migrations that have already landed. Its
three-column meaning is carried forward unchanged by the active, globally ordered
`V20260805170000__backtest_run_outcome_detail.sql` contribution.

`V20260802094500__backtest_run_input_pins.sql.fixture` is the historical
consumer-owned singular pin-table proposal. The root provider now owns the normalized
`V20260805130000__backtest_run_input_pins.sql` migration, so assembling both would
attempt to create `backtest.run_input_pins` twice. The fixture preserves the original
bytes but must never be restored to the active directory, and its retired
`dataset_manifest_id`, `dataset_hash`, and `feature_materialization_version` shape
must not return.
