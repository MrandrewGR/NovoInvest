#!/bin/bash
set -e

# Ожидание запуска зависимых сервисов
/app/wait-for-services.sh ni-postgres 5432 ni-kafka 9092

# Запуск supervisord
exec supervisord -c /app/supervisord.conf
