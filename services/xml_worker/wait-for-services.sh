# File location: ./services/xml_worker/wait-for-services.sh

#!/usr/bin/env bash
# Скрипт ожидания, пока Kafka станет доступна.

set -e

HOST="${KAFKA_HOST:-kafka}"
PORT="${KAFKA_PORT:-9092}"

echo "Ожидаем доступности Kafka на ${HOST}:${PORT}..."

while ! nc -z $HOST $PORT; do
  sleep 1
  echo "Ждем, пока Kafka поднимется..."
done

echo "Kafka доступна. Запускаем приложение."
exec "$@"
