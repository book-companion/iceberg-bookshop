PYTHON ?= .venv/bin/python
.PHONY: help setup setup-agent setup-spark seed example test server agent
help:
	@echo 'setup, seed, example, test; setup-agent/server/agent for MCP and model work; setup-spark for desktop probes'
setup:
	python3.13 -m venv .venv
	$(PYTHON) -m pip install -e '.[test]'
setup-agent: setup
	$(PYTHON) -m pip install -e '.[agent]'
setup-spark: setup
	$(PYTHON) -m pip install -e '.[spark]'
seed:
	$(PYTHON) -m iceberg_agents.cli seed
example:
	$(PYTHON) -m iceberg_agents.cli query --principal finance --metric net_revenue
test:
	$(PYTHON) -m pytest -m 'not stack and not model'
server:
	$(PYTHON) -m iceberg_agents.server
agent:
	$(PYTHON) -m iceberg_agents.agent
