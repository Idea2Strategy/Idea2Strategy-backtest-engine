import copy
import json
from pathlib import Path

import pytest

from backtest_engine.contracts import (
    ContractValidationError,
    validate_backtest_request,
    validate_backtest_result,
    validate_dataset_manifest,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json"


@pytest.fixture
def fixtures() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_backtest_consumer_accepts_the_d_owned_fixture_set(
    fixtures: dict[str, object],
) -> None:
    manifest = fixtures["dataset_manifest"]
    request = fixtures["backtest_request"]
    results = fixtures["backtest_results"]

    validate_dataset_manifest(manifest)
    validate_backtest_request(request)
    for result in results:
        validate_backtest_result(result)

    assert request["dataset_manifest_id"] == manifest["manifest_id"]
    assert request["dataset_hash"] == manifest["dataset_hash"]


def test_backtest_consumer_rejects_corrupted_manifest_hash(
    fixtures: dict[str, object],
) -> None:
    manifest = copy.deepcopy(fixtures["dataset_manifest"])
    manifest["dataset_hash"] = "0" * 64

    with pytest.raises(ContractValidationError, match="dataset_hash"):
        validate_dataset_manifest(manifest)


def test_terminal_result_requires_status_specific_detail(
    fixtures: dict[str, object],
) -> None:
    complete = copy.deepcopy(fixtures["backtest_results"][2])
    complete.pop("result_manifest_id")

    with pytest.raises(ContractValidationError, match="result_manifest_id"):
        validate_backtest_result(complete)


def test_backtest_consumer_rejects_unknown_result_version(
    fixtures: dict[str, object],
) -> None:
    queued = copy.deepcopy(fixtures["backtest_results"][0])
    queued["schema_version"] = 2

    with pytest.raises(ContractValidationError, match="schema_version"):
        validate_backtest_result(queued)
