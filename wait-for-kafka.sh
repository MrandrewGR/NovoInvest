#!/bin/bash
# wait-for-kafka.sh

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for Kafka at $host:$port..."

while ! nc -z "$host" "$port"; do
  echo "Kafka is unavailable - sleeping"
  sleep 1
done

echo "Kafka is up - executing command"
exec $cmd
