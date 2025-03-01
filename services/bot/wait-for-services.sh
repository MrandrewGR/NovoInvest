#!/bin/bash
set -e

cd /app

wait_for_service() {
  local host=$1
  local port=$2
  echo "Ожидание $host:$port..."
  while ! nc -z "$host" "$port"; do
    echo "$host недоступен - ожидание"
    sleep 1
  done
  echo "$host доступен - продолжаем"
}

# Ожидание PostgreSQL (host=ni-postgres, port=5432)
wait_for_service ni-postgres 5432

# Ожидание Kafka (host=ni-kafka, port=9092)
wait_for_service ni-kafka 9092

exec "$@"
