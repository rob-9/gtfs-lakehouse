.DEFAULT_GOAL := test

UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
RUN = UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) run --project poller --locked python -m gtfs_lakehouse

.PHONY: setup test

setup:
	cd poller && UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) sync --locked --extra test

test:
	cd poller && UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) run --locked --extra test python -m pytest

.PHONY: up down status fixtures poll-fixtures smoke-platform
up:
	docker compose up -d --wait --wait-timeout 180
	$(RUN) bootstrap

down:
	docker compose down

status:
	docker compose ps

fixtures:
	$(RUN) fixtures

poll-fixtures:
	$(RUN) poll --config config/feeds.toml

smoke-platform:
	$(RUN) smoke-platform
