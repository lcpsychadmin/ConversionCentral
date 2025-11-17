#!/bin/sh
set -e

CERT_PATH=${SSL_CERT_PATH:-/etc/ssl/custom/server.crt}
KEY_PATH=${SSL_KEY_PATH:-/etc/ssl/custom/server.key}
CERT_DIR=$(dirname "${CERT_PATH}")

if [ ! -f "${CERT_PATH}" ] || [ ! -f "${KEY_PATH}" ]; then
  echo "[proxy] No TLS certificate found at ${CERT_PATH} or key at ${KEY_PATH}. Generating self-signed certificate for localhost..."
  mkdir -p "${CERT_DIR}"
  openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "${KEY_PATH}" \
    -out "${CERT_PATH}" \
    -days ${SSL_SELF_SIGNED_DAYS:-365} \
    -subj "${SSL_SELF_SIGNED_SUBJECT:-/CN=localhost}"
  chmod 600 "${KEY_PATH}"
fi

exec "$@"
