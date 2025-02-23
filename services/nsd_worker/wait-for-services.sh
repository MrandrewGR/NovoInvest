#!/usr/bin/env bash
set -e

# Упрощённый вариант wait-for-it.sh
# Использование:
#   wait-for-it.sh host:port [--timeout=seconds] [-- command args]
#
# Пример:
#   wait-for-it.sh postgres:5432 --timeout=60 -- python app/main.py

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 host:port [--timeout=seconds] [-- command args]"
  exit 1
fi

# Извлекаем host и port из первого аргумента (формат host:port)
hostport="$1"
IFS=':' read -r host port <<< "$hostport"
shift

# Таймаут по умолчанию (секунды)
timeout=15

# Обработка аргументов (например, --timeout)
while [ "$#" -gt 0 ]; do
  case "$1" in
    --timeout=*)
      timeout="${1#*=}"
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

echo "Waiting for $host:$port up to $timeout seconds..."
start_ts=$(date +%s)

# Ожидание доступности сервиса
while ! nc -z "$host" "$port"; do
  sleep 1
  current_ts=$(date +%s)
  elapsed=$(( current_ts - start_ts ))
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "Timeout after $timeout seconds waiting for $host:$port"
    exit 1
  fi
done

echo "$host:$port is available."

# Если указаны дополнительные аргументы, выполняем их
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
