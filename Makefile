SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
REFERENCIA ?= test/referencia.json
export LOG_LEVEL ?= INFO

# Configuracion del chaos monkey (make chaos). Se pueden pisar al invocar:
# make chaos CHAOS_INTERVAL=5 CHAOS_TARGETS=splitter,aggregation
export CHAOS_INTERVAL ?= 15
export CHAOS_KILLS_PER_ROUND ?= 1
export CHAOS_START_DELAY ?= 0
export CHAOS_TARGETS ?= filter,filter_language,filter_year,filter_average,image_getter,splitter,aggregation,average,join
export CHAOS_SEED ?=

.PHONY: setup up logs test run verify down chaos

setup:
	@test -n "$(strip $(GB))" || { echo 'Falta GB. Uso: make setup GB=1.5' >&2; exit 1; }
	@mkdir -p datasets "$$(dirname "$(REFERENCIA)")"
	@workdir=$$(mktemp -d datasets/.setup-XXXXXX) && \
	trap 'rm -rf "$$workdir"' EXIT && \
	python3 tools/download_wikipedia.py --gigabytes "$(GB)" --output "$$workdir/descarga" && \
	python3 tools/serial.py "$$workdir/descarga/wikipedia.jsonl" --output "$$workdir/referencia.json" && \
	mv "$$workdir/descarga/wikipedia.jsonl" datasets/1.jsonl && \
	cp datasets/1.jsonl datasets/2.jsonl && \
	cp datasets/1.jsonl datasets/3.jsonl && \
	mv "$$workdir/referencia.json" "$(REFERENCIA)"

up:
	docker compose --progress quiet up --build --quiet-build --menu=false --remove-orphans --detach 2>&1 | tee ejecucion.log

logs:
	docker compose logs --follow

test: up
	@docker compose logs --follow 2>&1 & \
	logs_pid=$$!; \
	python3 tools/verificar.py --referencia "$(REFERENCIA)" --compose docker-compose.yaml --wait > verificacion.log 2>&1; \
	status=$$?; \
	cat verificacion.log; \
	docker compose logs >> ejecucion.log; \
	docker compose --progress quiet down || status=$$?; \
	wait "$$logs_pid" 2>/dev/null; \
	exit $$status

run: test

verify:
	python3 tools/verificar.py --referencia "$(REFERENCIA)" --compose docker-compose.yaml

down:
	docker compose down

chaos:
	python3 tools/chaos_monkey.py
