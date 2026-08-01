from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from backtest_engine.attempt_coordinator import AttemptFailure, FailureKind
from d_reproducibility_testkit import (
    BACKTEST_MEMORY_BYTES,
    FIXED_ALPACA_RESPONSE,
    BacktestComputeSample,
    PipelineComputePolicy,
    PipelineResourceLimitExceeded,
    materialize_fixed_alpaca_response,
    run_official_backtest,
)


def test_fixed_alpaca_to_official_backtest_is_reproducible(tmp_path: Path) -> None:
    first_input = materialize_fixed_alpaca_response(
        FIXED_ALPACA_RESPONSE,
        tmp_path / "first" / "market-bars.parquet",
        PipelineComputePolicy(max_input_rows=10, max_output_bytes=1_000_000),
    )
    second_input = materialize_fixed_alpaca_response(
        tuple(reversed(FIXED_ALPACA_RESPONSE)),
        tmp_path / "second" / "market-bars.parquet",
        PipelineComputePolicy(max_input_rows=10, max_output_bytes=1_000_000),
    )

    first = run_official_backtest(first_input)
    second = run_official_backtest(second_input)

    assert first_input.manifest == second_input.manifest
    assert first_input.parquet_bytes == second_input.parquet_bytes
    assert first.request["input_bundle_fingerprint"] == second.request[
        "input_bundle_fingerprint"
    ]
    assert (
        first.result.run_snapshot.snapshot_id
        == second.result.run_snapshot.snapshot_id
    )
    assert first.result.manifest == second.result.manifest
    assert first.result.object_bytes == second.result.object_bytes
    assert first.details.manifest == second.details.manifest
    assert [item.parquet_bytes for item in first.details.objects] == [
        item.parquet_bytes for item in second.details.objects
    ]
    assert first.result.summary.fill_count == 1
    assert first.attempt_completed


@pytest.mark.parametrize(
    "policy",
    [
        PipelineComputePolicy(max_input_rows=1, max_output_bytes=1_000_000),
        PipelineComputePolicy(max_input_rows=10, max_output_bytes=1),
    ],
)
def test_pipeline_compute_limit_blocks_publication(
    tmp_path: Path,
    policy: PipelineComputePolicy,
) -> None:
    target = tmp_path / "market-bars.parquet"

    with pytest.raises(PipelineResourceLimitExceeded):
        materialize_fixed_alpaca_response(FIXED_ALPACA_RESPONSE, target, policy)

    assert not target.exists()


@pytest.mark.parametrize(
    ("sample", "expected"),
    [
        (
            BacktestComputeSample(
                cpu_time=timedelta(minutes=5, microseconds=1),
                memory_bytes=BACKTEST_MEMORY_BYTES,
            ),
            FailureKind.CPU_LIMIT,
        ),
        (
            BacktestComputeSample(
                cpu_time=timedelta(seconds=1),
                memory_bytes=BACKTEST_MEMORY_BYTES + 1,
            ),
            FailureKind.MEMORY_LIMIT,
        ),
    ],
)
def test_backtest_compute_limit_stops_before_official_result(
    tmp_path: Path,
    sample: BacktestComputeSample,
    expected: FailureKind,
) -> None:
    pipeline_input = materialize_fixed_alpaca_response(
        FIXED_ALPACA_RESPONSE,
        tmp_path / "market-bars.parquet",
        PipelineComputePolicy(max_input_rows=10, max_output_bytes=1_000_000),
    )

    with pytest.raises(AttemptFailure) as failure:
        run_official_backtest(pipeline_input, compute_sample=sample)

    assert failure.value.kind is expected
