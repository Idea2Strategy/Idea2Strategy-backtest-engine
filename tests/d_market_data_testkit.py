"""Independent deterministic market-data fixture kit for D backtest tests."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class SmallParquetFixture:
    path: Path
    content_hash: str
    row_count: int
    schema_version: str


def write_small_market_bars(path: Path) -> SmallParquetFixture:
    """Write two ordered 30-minute bars with the official minimal columns."""

    schema_version = "market-bars-v2"
    schema = pa.schema(
        [
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("provider_symbol", pa.string(), nullable=False),
            pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("session_date_et", pa.date32(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.int64(), nullable=False),
        ],
        metadata={b"schema_version": schema_version.encode("ascii")},
    )
    table = pa.Table.from_pylist(
        [
            {
                "instrument_id": "11111111-1111-4111-8111-111111111111",
                "provider_symbol": "AAPL",
                "bar_start_at": datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                "session_date_et": date(2024, 1, 2),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000,
            },
            {
                "instrument_id": "11111111-1111-4111-8111-111111111111",
                "provider_symbol": "AAPL",
                "bar_start_at": datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc),
                "session_date_et": date(2024, 1, 2),
                "open": 101.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
                "volume": 1200,
            },
        ],
        schema=schema,
    )
    pq.write_table(table, path, compression="zstd", version="2.6")
    return SmallParquetFixture(
        path=path,
        content_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
        row_count=table.num_rows,
        schema_version=schema_version,
    )
