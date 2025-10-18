#!/usr/bin/env bash
set -euo pipefail

# Wait for dependent services if the optional wait-for-it script is provided
if [ -x "docker/backend/wait-for-it.sh" ]; then
  docker/backend/wait-for-it.sh postgres 5432
  docker/backend/wait-for-it.sh sqlserver 1433
fi

# Run database migrations before starting the API
alembic upgrade head

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
