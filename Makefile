# The companion to the Apache Iceberg series. One virtualenv and one shared
# `bookshop` package at the root; one folder per book. The Book 1 targets
# stay at the root, exactly as its chapters print them.
.DEFAULT_GOAL := help
PY := .venv/bin/python

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo "  Book 2 lives in book-2/: make -C book-2 help"

venv:  ## Create the virtualenv and install pinned dependencies (both books)
	uv venv --python 3.13 .venv || python3 -m venv .venv
	VIRTUAL_ENV=$(PWD)/.venv uv pip install -e . || .venv/bin/pip install -e .

up:  ## Book 1: start the REST catalog and wait for it
	$(MAKE) -C book-1 up

seed:  ## Book 1: create and load the bookshop tables
	$(PY) -m bookshop.seed

down:  ## Book 1: stop the catalog (keeps the warehouse)
	$(MAKE) -C book-1 down

clean:  ## Book 1: stop the catalog and delete all data
	$(MAKE) -C book-1 clean

verify:  ## Book 1: run every verification script
	$(MAKE) -C book-1 verify

.PHONY: help venv up seed down clean verify
