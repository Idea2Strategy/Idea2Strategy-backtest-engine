import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from d_market_data_testkit import write_small_market_bars


def test_small_market_bars_fixture_is_deterministic_and_self_describing(
    tmp_path: Path,
) -> None:
    first = write_small_market_bars(tmp_path / "first.parquet")
    second = write_small_market_bars(tmp_path / "second.parquet")

    assert first.path.read_bytes() == second.path.read_bytes()
    assert first.content_hash == hashlib.sha256(first.path.read_bytes()).hexdigest()
    assert first.row_count == 2

    parquet = pq.ParquetFile(first.path)
    assert parquet.metadata.num_rows == first.row_count
    assert parquet.schema_arrow.metadata[b"schema_version"] == b"market-bars-v2"
    assert parquet.read(columns=["provider_symbol"])["provider_symbol"].to_pylist() == [
        "AAPL",
        "AAPL",
    ]
