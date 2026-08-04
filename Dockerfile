FROM python:3.12.13-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .

# The runtime does not invoke Perl. Debian marks perl-base Essential, but this
# slim image has no installed reverse dependency on it; removing it after the
# Python installation keeps the deployed attack surface free of unfixed Perl
# vulnerabilities without affecting package installation during the build.
RUN dpkg --remove --force-remove-essential perl-base

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8082

CMD ["backtest-api"]
