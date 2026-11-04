SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
.DEFAULT_GOAL := test
REFERENCIA ?= test/referencia-250000.json

.PHONY: up logs test run verify down

up:
	docker compose --progress quiet up --build --quiet-build --menu=false --remove-orphans --detach 2>&1 | tee ejecucion.log

logs:
	docker compose logs --follow

test: up
	@docker compose logs --follow 2>&1 & \
	logs_pid=$$!; \
	trap 'kill "$$logs_pid" 2>/dev/null || true' EXIT; \
	python3 tools/verificar.py --referencia "$(REFERENCIA)" --compose docker-compose.yaml --wait > verificacion.log 2>&1; \
	status=$$?; \
	kill "$$logs_pid" 2>/dev/null || true; \
	wait $$logs_pid 2>/dev/null; \
	trap - EXIT; \
	docker compose logs >> ejecucion.log; \
	docker compose down || status=$$?; \
	cat verificacion.log; \
	exit $$status

run: test

verify:
	python3 tools/verificar.py --referencia "$(REFERENCIA)" --compose docker-compose.yaml

down:
	docker compose down
