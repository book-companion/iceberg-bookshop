PYTHON ?= .venv/bin/python
.PHONY: setup setup-spark setup-postgres test pilot source-up
setup:
	python3.13 -m venv .venv
	$(PYTHON) -m pip install -e '.[test]'
setup-spark: setup
	$(PYTHON) -m pip install -e '.[spark]'
setup-postgres: setup
	$(PYTHON) -m pip install -e '.[postgres]'
test:
	$(PYTHON) -m pytest tests
pilot:
	$(PYTHON) -m iceberg_migration.cli --work work/pilot pilot
source-up:
	docker compose -p migration -f docker-compose.yml up -d
