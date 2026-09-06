# Book 2 — Apache Iceberg in the Wild. The platform the book runs on:
# SeaweedFS, the reference REST catalog on object storage, PostgreSQL 18,
# Polaris, Lakekeeper, Trino, Flink, Kafka and Debezium Connect. Run from
# this directory or with `make -C book-2 <target>` from the root.
.DEFAULT_GOAL := help
PY := ../.venv/bin/python

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

jars:  ## Fetch Flink's Iceberg runtime, AWS bundle, Hadoop client and Kafka connector
	./fetch-jars.sh

up: jars  ## Start the whole platform and wait for the catalogs, Trino and Flink
	mkdir -p fixture-catalog && chmod 777 fixture-catalog
	docker compose up -d
	@for svc in "catalog|http://localhost:8181/v1/config|endpoints" "polaris|http://localhost:8282/q/health|UP" "lakekeeper|http://localhost:8381/management/v1/info|version" "trino|http://localhost:8080/v1/info|\"starting\":false" "flink|http://localhost:8081/taskmanagers|\"id\"" "connect|http://localhost:8083/connectors|["; do \
	  name=$${svc%%|*}; rest=$${svc#*|}; url=$${rest%%|*}; token=$${rest#*|}; printf 'waiting for %s' $$name; \
	  for i in $$(seq 1 90); do curl -fsS $$url 2>/dev/null | grep -qF -- "$$token" && { echo " ready"; break; }; printf '.'; sleep 2; done; done
	@docker compose ps --format '  {{.Name}}\t{{.Status}}'

down:  ## Stop the platform (keeps every volume)
	docker compose down

clean:  ## Stop the platform and delete all data
	docker compose down -v
	rm -rf fixture-catalog

verify:  ## Run every verification script
	@for f in verify/*.py; do echo "\n===== book-2/$$f ====="; $(PY) $$f || exit 1; done

.PHONY: help jars up down clean verify
