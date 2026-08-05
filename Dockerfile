FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    I2S_MIGRATION_CONTRIBUTION_ROOT=/app/db/migration-contributions

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY db ./db

RUN pip install --no-cache-dir .

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
