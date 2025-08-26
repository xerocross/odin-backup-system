# Odin backup Makefile

# ---- Paths (tweak if you move things) ----
ROOT              := /home/adam/bin/odin-backup-system
AUDIT_DB          := /home/adam/.odin_backup/audit.db
MIGRATIONS		  := $(ROOT)/migrations


PY ?= $(ROOT)/venv/bin/python

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nOdin Backup Jobs:\n\n"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST); \
	echo ""

migrate: ## Run database migrations
	$(PY) $(ROOT)/migrate.py --db $(AUDIT_DB) --dir $(MIGRATIONS) up

run_git_pull: ## Execute the Git Pull job
	$(PY) $(ROOT)/odin_pull_git_updates.py

manifest: ## Execute the job to generate the Odin manifest.
	$(PY) $(ROOT)/odin_run_manifest_job.py
