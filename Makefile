.PHONY: db/init db/psql install-dev

# Run initial schema against local Postgres
# Example: make db/init PGURL=postgres://user:pass@localhost:5432/ir_db

PGURL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)

# Initialize database schema
# Usage: make db/init

db/init:
	psql "$(PGURL)" -f ddl/001_init.sql

# Open interactive psql shell
# Usage: make db/psql

db/psql:
	psql "$(PGURL)"

# Install dev dependencies into current virtualenv
install-dev:
	pip install -r requirements-dev.txt 