#!/usr/bin/env bash
set -euo pipefail

# Normalize DATABASE_URL if Heroku injects the old postgres:// scheme so SQLAlchemy
# loads the correct dialect. Normalize to postgresql+psycopg:// so psycopg3 is used.
if [ -n "${DATABASE_URL:-}" ] && [ "${DATABASE_URL#postgres://}" != "${DATABASE_URL}" ]; then
  DATABASE_URL="postgresql+psycopg://${DATABASE_URL#postgres://}"
  export DATABASE_URL
fi

# Wait for the database to accept connections when the helper script is present.
# Detect hostname/port from DATABASE_URL so the check works on platforms like Heroku.
if [ -x "docker/backend/wait-for-it.sh" ]; then
  DB_HOST="postgres"
  DB_PORT="5432"

  if [ -n "${DATABASE_URL:-}" ]; then
    host_port=$(python - <<'PY'
import os
from urllib.parse import urlparse

url = os.environ.get("DATABASE_URL", "")
if url:
    parsed = urlparse(url)
    host = parsed.hostname or "postgres"
    port = parsed.port or 5432
    print(f"{host}:{port}")
PY
)
    if [ -n "${host_port}" ]; then
      DB_HOST=${host_port%:*}
      DB_PORT=${host_port#*:}
    fi
    DB_HOST=${DB_HOST:-postgres}
    DB_PORT=${DB_PORT:-5432}
  fi

  echo "Waiting for ${DB_HOST}:${DB_PORT}..."
  docker/backend/wait-for-it.sh "${DB_HOST}" "${DB_PORT}" /bin/true
fi

# Run database migrations before starting the API
alembic upgrade head

UVICORN_PORT=${PORT:-8000}
exec uvicorn app.main:app --host 0.0.0.0 --port "${UVICORN_PORT}"
