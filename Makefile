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
	$(RUN) up

down:
	$(RUN) down

status:
	docker compose ps

fixtures:
	$(RUN) fixtures

poll-fixtures:
	$(RUN) poll --config config/feeds.toml

smoke-platform:
	$(RUN) smoke-platform

.PHONY: build submit load-fixtures serve query test-java
build:
	mvn -f flink-job/pom.xml -Dmaven.repo.local="$(CURDIR)/.m2" -q package

test-java:
	mvn -f flink-job/pom.xml -Dmaven.repo.local="$(CURDIR)/.m2" -q test

load-fixtures:
	$(RUN) load-fixture-schedule

submit: build
	$(RUN) submit

.PHONY: redeploy monitor quality observability benchmark-small
redeploy: build
	$(RUN) redeploy

monitor:
	$(RUN) monitor

quality:
	cd dbt && UV_CACHE_DIR="$(UV_CACHE_DIR)" $(UV) run --locked dbt build --profiles-dir .

observability:
	docker compose --profile observability up -d --build exporter prometheus grafana

benchmark-small:
	python3 benchmark/run.py --output benchmark/artifacts/run-$(shell date +%Y%m%d-%H%M%S)

.PHONY: test-recovery
test-recovery:
	$(RUN) test-recovery --output var/recovery-$(shell date +%Y%m%d-%H%M%S)

serve:
	$(RUN) serve

query:
	$(RUN) query

.PHONY: smoke
smoke:
	$(RUN) smoke

.PHONY: workers demo
workers:
	docker compose --profile workers up -d --build fixtures poller serving

demo:
	$(MAKE) up
	$(MAKE) load-fixtures
	$(MAKE) submit
	$(MAKE) workers
	$(MAKE) smoke
