# Book 5: S3 Tables. Nothing here calls AWS; see README.md.
.DEFAULT_GOAL := help
PY ?= python3

help:  ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

cost:  ## Print the illustrative bookshop cost estimate (standard library, offline)
	$(PY) cost/s3_tables_cost.py --prices cost/prices/us-east-1-2026-09-28.json --workload cost/workloads/bookshop.json

tf-check:  ## terraform fmt -check and validate (needs terraform; never plans or applies)
	cd terraform && terraform fmt -check -recursive && terraform init -backend=false -input=false >/dev/null && terraform validate

.PHONY: help cost tf-check
