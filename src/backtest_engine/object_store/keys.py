"""The canonical backtest result object key (D-REBUILD-SPEC 2.5).

```
backtest-results/<run_id>/<record_type>/week_start=<YYYY-MM-DD>/part=<NNNN>/<content_hash>.parquet
```

The key is the object's published identity, so it is built and parsed in exactly one
place. Every component is validated: `run_id` is a UUID, `week_start` is an **ET
Monday** (`backtest.detail_manifests.week_start_date`, whose note forbids a detail
object from crossing an ET Monday week boundary), `part_number` is 1-based and
zero-padded to four digits, and `content_hash` is a lowercase SHA-256 of the object
bytes. None of those components can contain a path separator or `..`, so a rendered
key can never traverse; `store` still guards raw strings because the `ObjectStore`
protocol accepts any string.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import date

from .errors import ObjectKeyError


__all__ = [
    "BACKTEST_RESULT_PREFIX",
    "MAX_PART_NUMBER",
    "PARQUET_SUFFIX",
    "BacktestObjectKey",
]


BACKTEST_RESULT_PREFIX = "backtest-results"
PARQUET_SUFFIX = ".parquet"
MAX_PART_NUMBER = 9999

_RECORD_TYPE = re.compile(r"^[A-Z][A-Z0-9_]{0,49}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_WEEK_START = re.compile(r"^week_start=(\d{4})-(\d{2})-(\d{2})$")
_PART = re.compile(r"^part=(\d{4})$")


@dataclass(frozen=True, slots=True, order=True)
class BacktestObjectKey:
    """One parsed canonical key. Ordering is (run, record type, week, part)."""

    run_id: str
    record_type: str
    week_start: date
    part_number: int
    content_hash: str

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "run_id", str(uuid.UUID(str(self.run_id))))
        except (ValueError, AttributeError, TypeError) as exc:
            raise ObjectKeyError(f"run_id must be a UUID, got {self.run_id!r}") from exc
        if not isinstance(self.record_type, str) or not _RECORD_TYPE.fullmatch(self.record_type):
            raise ObjectKeyError(
                f"record_type must be UPPER_SNAKE_CASE of at most 50 characters, got {self.record_type!r}"
            )
        if not isinstance(self.week_start, date):
            raise ObjectKeyError(f"week_start must be a date, got {self.week_start!r}")
        if self.week_start.weekday() != 0:
            raise ObjectKeyError(
                f"week_start must be a Monday (the ET detail week boundary), got {self.week_start.isoformat()}"
            )
        if (
            not isinstance(self.part_number, int)
            or isinstance(self.part_number, bool)
            or not 1 <= self.part_number <= MAX_PART_NUMBER
        ):
            raise ObjectKeyError(f"part_number must be between 1 and {MAX_PART_NUMBER}, got {self.part_number!r}")
        if not isinstance(self.content_hash, str) or not _SHA256.fullmatch(self.content_hash):
            raise ObjectKeyError(f"content_hash must be a lowercase SHA-256, got {self.content_hash!r}")

    def render(self) -> str:
        return (
            f"{BACKTEST_RESULT_PREFIX}/{self.run_id}/{self.record_type}/"
            f"week_start={self.week_start.isoformat()}/part={self.part_number:04d}/"
            f"{self.content_hash}{PARQUET_SUFFIX}"
        )

    def __str__(self) -> str:
        return self.render()

    @classmethod
    def parse(cls, key: str) -> BacktestObjectKey:
        if not isinstance(key, str):
            raise ObjectKeyError(f"object key must be a string, got {type(key).__name__}")
        parts = key.split("/")
        if len(parts) != 6:
            raise ObjectKeyError(f"object key must have exactly 6 segments, got {len(parts)}: {key!r}")
        prefix, run_id, record_type, week, part, filename = parts
        if prefix != BACKTEST_RESULT_PREFIX:
            raise ObjectKeyError(f"object key must start with {BACKTEST_RESULT_PREFIX!r}, got {prefix!r}")
        week_match = _WEEK_START.fullmatch(week)
        if week_match is None:
            raise ObjectKeyError(f"week segment must be 'week_start=YYYY-MM-DD', got {week!r}")
        part_match = _PART.fullmatch(part)
        if part_match is None:
            raise ObjectKeyError(f"part segment must be 'part=NNNN', got {part!r}")
        if not filename.endswith(PARQUET_SUFFIX):
            raise ObjectKeyError(f"object file name must end with {PARQUET_SUFFIX}, got {filename!r}")
        try:
            week_start = date(int(week_match.group(1)), int(week_match.group(2)), int(week_match.group(3)))
        except ValueError as exc:
            raise ObjectKeyError(f"week segment is not a real date: {week!r}") from exc
        return cls(
            run_id=run_id,
            record_type=record_type,
            week_start=week_start,
            part_number=int(part_match.group(1)),
            content_hash=filename[: -len(PARQUET_SUFFIX)],
        )
