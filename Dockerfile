FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    I2S_MIGRATION_CONTRIBUTION_ROOT=/app/db/migration-contributions

WORKDIR /app

# Third-party packages come only from the hash-locked runtime set, as wheels, so
# a rebuild installs the same bytes and a tampered or substituted file fails.
# See DEVELOPMENT.md for how the lock is regenerated.
COPY requirements/runtime.txt ./requirements/runtime.txt
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: \
        -r requirements/runtime.txt

COPY pyproject.toml README.md ./
COPY src ./src
COPY db ./db

# `--no-deps`: the project itself adds nothing that is not already locked, and
# `pip check` fails the build if pyproject.toml declares something the lock lacks.
RUN pip install --no-cache-dir --no-deps . \
    && pip check

# The runtime does not invoke Perl. Debian marks perl-base Essential, but this
# slim image has no installed reverse dependency on it. Purge it after the
# Python installation and remove dpkg's previous-status snapshot so scanners
# cannot treat the removed source package as part of the final filesystem.
RUN dpkg --purge --force-remove-essential perl-base \
    && rm -f /var/lib/dpkg/status-old

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8082

CMD ["backtest-api"]
