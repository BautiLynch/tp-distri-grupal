SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
.DEFAULT_GOAL := test
REFERENCIA ?= test/referencia.json
ARTICULOS ?= 400000

.PHONY: setup up logs test run verify down

setup:
	@[[ "$(ARTICULOS)" =~ ^[1-9][0-9]*$$ ]] || { echo "ARTICULOS debe ser un entero positivo"; exit 1; }
	@mkdir -p datasets "$$(dirname "$(REFERENCIA)")"
	@set -e; \
	workdir=$$(mktemp -d datasets/.setup-XXXXXX); \
	trap 'rm -rf "$$workdir"' EXIT; \
	python3 tools/download_wikipedia.py --articles "$(ARTICULOS)" --output "$$workdir"; \
	mv "$$workdir/wikipedia-$(ARTICULOS).jsonl" "$$workdir/1.jsonl"; \
	python3 tools/serial.py "$$workdir/1.jsonl" --output "$$workdir/referencia.json"; \
	mv "$$workdir/1.jsonl" datasets/1.jsonl; \
	cp datasets/1.jsonl datasets/2.jsonl; \
	cp datasets/1.jsonl datasets/3.jsonl; \
	mv "$$workdir/referencia.json" "$(REFERENCIA)"

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
	cat verificacion.log; \
	docker compose logs >> ejecucion.log; \
	docker compose --progress quiet down || status=$$?; \
	wait $$logs_pid 2>/dev/null; \
	trap - EXIT; \
	exit $$status

run: test

verify:
	python3 tools/verificar.py --referencia "$(REFERENCIA)" --compose docker-compose.yaml

down:
	docker compose down
