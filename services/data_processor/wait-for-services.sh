# File location: services/data_processor/wait-for-services.sh
#!/bin/bash
set -e

wait_for_service() {
  local host=$1
  local port=$2
  echo "Waiting for $host:$port..."
  while ! nc -z "$host" "$port"; do
    echo "$host is unavailable - sleeping"
    sleep 1
  done
  echo "$host is up - continuing"
}

# Ожидание PostgreSQL
wait_for_service postgres 5432

# Ожидание Kafka
wait_for_service kafka 9092

exec "$@"
