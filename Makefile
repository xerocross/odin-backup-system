# Odin backup Makefile

# ---- Paths (tweak if you move things) ----
CONFIG_FILE              := /home/adam/.config/odin-backup-system/root.txt
DB_LOCATION_CONFIG       := /home/adam/.config/odin-backup-system/audit_db.txt
ROOT                     := $(shell head -n 1 $(CONFIG_FILE))
AUDIT_DB                 := $(shell head -n 1 $(DB_LOCATION_CONFIG))
MIGRATIONS		 := $(ROOT)/migrations


PY ?= $(ROOT)/.venv/bin/python

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nOdin Backup Jobs:\n\n"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST); \
	echo ""

migrate: ## Run database migrations
	$(PY) $(ROOT)/scripts/migrate.py --db $(AUDIT_DB) --dir $(MIGRATIONS) up

run_git_pull: ## Execute the Git Pull job
	$(PY) $(ROOT)/odin_pull_git_updates.py

root: ## display root
	echo $(ROOT)

manifest: ## Execute the job to generate the Odin manifest.
	$(PY) $(ROOT)/odin_run_manifest_job.py
