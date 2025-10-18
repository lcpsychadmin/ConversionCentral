#!/usr/bin/env bash
# Simplified wait-for-it script to wait for a TCP host:port
set -e

HOST="$1"
shift
PORT="$1"
shift
TIMEOUT=60

while ! nc -z "$HOST" "$PORT"; do
  echo "Waiting for $HOST:$PORT..."
  TIMEOUT=$((TIMEOUT-1))
  if [ "$TIMEOUT" -le 0 ]; then
    echo "Timeout waiting for $HOST:$PORT" >&2
    exit 1
  fi
  sleep 1
done

exec "$@"
