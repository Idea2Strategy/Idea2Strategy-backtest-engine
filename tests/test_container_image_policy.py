"""Regression checks for the deployable backtest container base image."""

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
APPROVED_BASE_IMAGE = (
    "python:3.12.13-slim-bookworm"
    "@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b"
)


def test_dockerfile_uses_the_reproducible_bookworm_base_image() -> None:
    dockerfile_lines = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8").splitlines()

    assert dockerfile_lines[0] == f"FROM {APPROVED_BASE_IMAGE}"


def test_dockerfile_purges_runtime_unused_perl_and_package_metadata_after_installation() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")

    install_position = dockerfile.index("RUN pip install --no-cache-dir .")
    purge_position = dockerfile.index("RUN dpkg --purge --force-remove-essential perl-base")

    assert purge_position > install_position
    assert "rm -f /var/lib/dpkg/status-old" in dockerfile


def test_dockerfile_carries_the_migration_contract_runtime_verification_needs() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY db ./db" in dockerfile
    assert "I2S_MIGRATION_CONTRIBUTION_ROOT=/app/db/migration-contributions" in dockerfile
