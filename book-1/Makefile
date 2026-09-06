# Book 1 — Apache Iceberg from the Ground Up. The root Makefile delegates
# here, so `make up && make seed && make verify` at the repository root
# still does exactly what chapter 1 prints.
.DEFAULT_GOAL := help
PY := ../.venv/bin/python

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up:  ## Start the REST catalog and wait for it
	mkdir -p warehouse catalog
	@echo "WAREHOUSE_DIR=$(CURDIR)/warehouse" > .env
	docker compose up -d
	@printf 'waiting for the catalog'
	@for i in $$(seq 1 40); do \
	  curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1 && { echo " ready"; exit 0; }; \
	  printf '.'; sleep 2; done; echo " TIMED OUT"; exit 1

down:  ## Stop the catalog (keeps the warehouse)
	docker compose down

clean:  ## Stop the catalog and delete all data
	docker compose down -v
	rm -rf warehouse catalog ../labs

verify:  ## Run every verification script
	@for f in verify/*.py; do echo "\n===== book-1/$$f ====="; $(PY) $$f || exit 1; done

.PHONY: help up down clean verify
