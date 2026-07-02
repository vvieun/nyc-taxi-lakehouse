#!/usr/bin/env bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --firstname admin \
  --lastname admin \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

superset init

exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 120 \
  "superset.app:create_app()"
