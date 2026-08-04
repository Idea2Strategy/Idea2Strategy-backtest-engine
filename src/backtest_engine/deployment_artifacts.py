"""Offline validation and byte hashes for deploy-time policy artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from backtest_engine.production import load_execution_policy_catalog, load_runtime_policy


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_report(execution_path: Path, runtime_path: Path) -> dict[str, object]:
    """Validate both runtime inputs and report hashes of their exact object bytes."""

    execution = load_execution_policy_catalog(execution_path)
    runtime = load_runtime_policy(runtime_path)
    return {
        "execution-policy": {
            "policyVersions": list(execution.versions),
            "schemaVersion": 1,
            "sha256": _sha256(execution_path),
        },
        "runtime-policy": {
            "fractionalPolicyVersion": runtime.fractional.policy_version,
            "microstructureVersion": runtime.microstructure.version,
            "schemaVersion": 1,
            "sha256": _sha256(runtime_path),
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate backtest deployment policies and emit exact S3-object SHA-256 values."
    )
    parser.add_argument("--execution-policy", required=True, type=Path)
    parser.add_argument("--runtime-policy", required=True, type=Path)
    args = parser.parse_args(argv)
    print(json.dumps(artifact_report(args.execution_policy, args.runtime_policy), indent=2, sort_keys=True))
    return 0


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":  # pragma: no cover - console-script boundary
    run()
