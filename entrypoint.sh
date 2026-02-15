#!/bin/sh
set -e

DB_PATH="${DB_PATH:-/data/output.db}"
PORT="${PORT:-8080}"

if [ ! -f "$DB_PATH" ]; then
  echo "Database not found at $DB_PATH. Generating (this may take several minutes)..."
  cd /app
  ./cpg-gen -modules './client_golang:github.com/prometheus/client_golang:client_golang,./prometheus-adapter:sigs.k8s.io/prometheus-adapter:adapter,./alertmanager:github.com/prometheus/alertmanager:alertmanager' ./prometheus "$DB_PATH"
  echo "Database generated."
fi

exec /server -db "$DB_PATH" -port "$PORT" -static /static
