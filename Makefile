.DEFAULT_GOAL := test

UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache

.PHONY: setup test

setup:
	cd poller && UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) sync --extra test

test:
	cd poller && UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) run --extra test python -m pytest
	python3 -m json.tool schemas/gtfs_event.avsc >/dev/null
	python3 -m json.tool schemas/route_metric.avsc >/dev/null
