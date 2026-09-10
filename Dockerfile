FROM ghcr.io/astral-sh/uv:0.8.17 AS uv
FROM python:3.13.5-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY poller/pyproject.toml poller/uv.lock /app/poller/
RUN uv sync --project poller --locked --no-install-project
COPY poller/src /app/poller/src
COPY schemas /app/schemas
COPY tests/fixtures /app/tests/fixtures
COPY clickhouse /app/clickhouse
COPY config /app/config
RUN uv sync --project poller --locked
ENTRYPOINT ["/app/poller/.venv/bin/python", "-m", "gtfs_lakehouse"]
