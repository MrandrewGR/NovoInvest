#!/bin/bash
set -e

cd /app

wait_for_service() {
  local host=$1
  local port=$2
  echo "Waiting for $host:$port..."
  while ! nc -z "$host" "$port"; do
    echo "$host is unavailable - sleeping"
    sleep 4
  done
  echo "$host is up - continuing"
  sleep 4
}

# Ожидание PostgreSQL (host=postgres, port=5432)
wait_for_service postgres 5432

# Ожидание Kafka (host=kafka, port=9092)
wait_for_service kafka 9092

exec "$@"