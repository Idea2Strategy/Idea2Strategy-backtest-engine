from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backtest_engine.deployment_artifacts import artifact_report, main
from backtest_engine.production import ConfigurationError, load_runtime_policy


def _execution_policy() -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "policies": [
            {
                "version": "policy-v1",
                "releaseQuarter": "2024-Q1",
                "periodStart": "2024-01-01T05:00:00Z",
                "periodEnd": "2024-04-01T04:00:00Z",
                "feeRate": "0.002",
                "slippageRateBps": 5,
                "timezone": "America/New_York",
                "sessionCalendar": "XNYS",
                "timestampUnit": "us",
                "priceArrowType": "double",
                "volumeArrowType": "int64",
                "marketDataSchemaVersion": "market-bars-v2",
                "calculationModelVersion": "backtest-calculation-v1",
                "marketRulesVersion": "market:1.0.0",
                "accountingRulesVersion": "accounting:1.0.0",
                "precisionRulesVersion": "precision:1.0.0",
                "feePolicyId": "00000000-0000-4000-8000-000000000001",
                "buyingPowerBufferPolicyId": "00000000-0000-4000-8000-000000000001",
                "goodTillCancelledHorizonSeconds": 7776000,
                "maxOrderHorizonSeconds": 7776000,
            }
        ],
    }


def _runtime_policy() -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "attempt": {
            "maxAttempts": 3,
            "leaseDurationSeconds": 300,
            "attemptTimeoutSeconds": 1800,
            "maxCpuTimeSeconds": 300,
            "maxMemoryBytes": 536870912,
        },
        "microstructure": {
            "version": "microstructure-v1",
            "maxVolumeParticipationBps": 1000,
            "buyingPowerBufferPolicyId": "00000000-0000-4000-8000-000000000001",
            "buyingPowerBufferBps": 1,
        },
        "fractional": {"policyVersion": "fractional-v1", "instrumentIds": []},
        "riskLimits": {
            "maxStrategyNotional": "100000.00",
            "maxGrossExposure": "100000.00",
            "maxInstrumentExposure": "10000.00",
        },
    }


def _write(path: Path, document: dict[str, object]) -> bytes:
    raw = (json.dumps(document, indent=2) + "\n").encode()
    path.write_bytes(raw)
    return raw


def test_artifact_report_validates_runtime_shapes_and_hashes_exact_file_bytes(tmp_path: Path) -> None:
    execution = tmp_path / "execution-policy.json"
    runtime = tmp_path / "runtime-policy.json"
    execution_bytes = _write(execution, _execution_policy())
    runtime_bytes = _write(runtime, _runtime_policy())

    report = artifact_report(execution, runtime)

    assert report == {
        "execution-policy": {
            "policyVersions": ["policy-v1"],
            "schemaVersion": 1,
            "sha256": hashlib.sha256(execution_bytes).hexdigest(),
        },
        "runtime-policy": {
            "fractionalPolicyVersion": "fractional-v1",
            "microstructureVersion": "microstructure-v1",
            "schemaVersion": 1,
            "sha256": hashlib.sha256(runtime_bytes).hexdigest(),
        },
    }


def test_cli_output_is_stable_and_does_not_embed_machine_specific_paths(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    execution = tmp_path / "execution-policy.json"
    runtime = tmp_path / "runtime-policy.json"
    _write(execution, _execution_policy())
    _write(runtime, _runtime_policy())

    assert main(["--execution-policy", str(execution), "--runtime-policy", str(runtime)]) == 0
    first = capsys.readouterr().out
    assert main(["--execution-policy", str(execution), "--runtime-policy", str(runtime)]) == 0

    assert capsys.readouterr().out == first
    assert str(tmp_path) not in first
    assert first == json.dumps(json.loads(first), indent=2, sort_keys=True) + "\n"


def test_runtime_policy_validation_rejects_values_runtime_cannot_enforce(tmp_path: Path) -> None:
    runtime = _runtime_policy()
    runtime["attempt"]["maxMemoryBytes"] = 0  # type: ignore[index]
    path = tmp_path / "runtime-policy.json"
    _write(path, runtime)

    with pytest.raises(ConfigurationError, match="maxMemoryBytes must be positive"):
        load_runtime_policy(path)
