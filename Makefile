.DEFAULT_GOAL := test

UV ?= uv
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache

.PHONY: setup test

setup:
	cd poller && UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) sync --locked --extra test

test:
	cd poller && UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) run --locked --extra test python -m pytest
