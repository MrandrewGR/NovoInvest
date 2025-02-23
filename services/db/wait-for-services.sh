#!/bin/bash
set -e

cd /app

wait_for_service() {
  local host=$1
  local port=$2
  echo "Waiting for $host:$port..."
  while ! nc -z "$host" "$port"; do
    echo "$host:$port is unavailable - sleeping"
    sleep 4
  done
  echo "$host:$port is up - continuing"
  sleep 4
}

# Iterate over arguments in pairs (host port)
while [[ $# -gt 0 ]]
do
  host="$1"
  port="$2"
  wait_for_service "$host" "$port"
  shift 2
done

exec "$@"
