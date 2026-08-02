"""Filesystem helpers that keep local object IO inside the Windows `MAX_PATH` limit.

Canonical backtest object keys are deep by contract
(`backtest-results/<run_id>/<record_type>/week_start=.../part=.../<64 hex>.parquet`
is roughly 150 characters before the store root is prepended), so a store rooted under
a normal Windows user profile can exceed 260 characters before the file name is even
appended. Shortening the key is not an option: the key is the published identity.

Two independent problems, same as the sibling `market_pipeline_lib.fs_paths`:

* `long_path` renders an absolute path in Windows extended-length form for the
  duration of a single OS call.
* `short_temp_path` bounds the *transient* staging name so an atomic write does not
  add the destination's own length again.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path


__all__ = [
    "MAX_TEMP_SUFFIX_LENGTH",
    "TEMP_TOKEN_BYTES",
    "long_path",
    "short_temp_path",
]


# ".<32 hex chars>.tmp" would be 37; 8 random bytes gives ".<16 hex>.tmp" = 21.
TEMP_TOKEN_BYTES = 8
MAX_TEMP_SUFFIX_LENGTH = 2 + 2 * TEMP_TOKEN_BYTES + 4

_WINDOWS = os.name == "nt"
_EXTENDED_PREFIX = "\\\\?\\"
_DEVICE_PREFIX = "\\\\.\\"


def short_temp_path(destination: Path) -> Path:
    """A sibling staging path whose name length is fixed, hidden and unique per call."""

    return destination.parent / f".{secrets.token_hex(TEMP_TOKEN_BYTES)}.tmp"


def long_path(path: Path | str) -> str:
    """Return a path string safe to hand to an OS call on Windows.

    Everywhere but Windows, and for paths that already carry an extended-length or
    device prefix, the input is returned unchanged.
    """

    text = os.fspath(path)
    if not _WINDOWS:
        return text
    if text.startswith((_EXTENDED_PREFIX, _DEVICE_PREFIX)):
        return text
    # abspath, not Path.resolve(): resolve() also follows symlinks and reparse points,
    # which would rewrite the caller's chosen path rather than just lengthening it.
    absolute = os.path.abspath(text)  # noqa: PTH100
    if absolute.startswith("\\\\"):
        # \\server\share -> \\?\UNC\server\share
        return f"{_EXTENDED_PREFIX}UNC{absolute[1:]}"
    return _EXTENDED_PREFIX + absolute
