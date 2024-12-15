#!/bin/bash
# wait-for-kafka.sh

HOST=$1
PORT=$2

echo "Waiting for Kafka at $HOST:$PORT..."

while ! nc -z $HOST $PORT; do
  echo "Kafka is unavailable - sleeping"
  sleep 5
done

echo "Kafka is up - executing command"
exec "$@"
