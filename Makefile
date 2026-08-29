.DEFAULT_GOAL := help
PY := .venv/bin/python

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv:  ## Create the virtualenv and install pinned dependencies
	uv venv --python 3.13 .venv || python3 -m venv .venv
	VIRTUAL_ENV=$(PWD)/.venv uv pip install -e . || .venv/bin/pip install -e .

up:  ## Start the REST catalog and wait for it
	mkdir -p warehouse catalog
	@echo "WAREHOUSE_DIR=$(PWD)/warehouse" > .env
	docker compose up -d
	@printf 'waiting for the catalog'
	@for i in $$(seq 1 40); do \
	  curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1 && { echo " ready"; exit 0; }; \
	  printf '.'; sleep 2; done; echo " TIMED OUT"; exit 1

seed:  ## Create and load the bookshop tables
	$(PY) -m bookshop.seed

down:  ## Stop the catalog (keeps the warehouse)
	docker compose down

clean:  ## Stop the catalog and delete all data
	docker compose down -v
	rm -rf warehouse catalog labs

verify:  ## Run every verification script
	@for f in verify/*.py; do echo "\n===== $$f ====="; $(PY) $$f || exit 1; done

.PHONY: help venv up seed down clean verify
