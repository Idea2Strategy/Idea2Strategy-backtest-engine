# Pending root migration fixture

This directory is reserved for provider-owned schemas that have been observed but
have not yet reached the central migration bundle. It is currently empty because
`run_input_pins` is now present in that bundle and its fixture moved to
`../central-migration/`.

Files placed here are not Flyway contributions, are not approved canonical source,
and must not be deployed from this repository. The historical
`V20260802094500__backtest_run_input_pins.sql` contribution is preserved byte-for-byte
under `../superseded-proposals/` and is excluded from current test assembly because its consumer-owned singular
dataset/feature shape was superseded by the normalized provider-owned bundle design.
