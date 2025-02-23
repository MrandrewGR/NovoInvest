#!/bin/bash
set -e

# Ожидание запуска зависимых сервисов
/app/wait-for-services.sh postgres 5432 kafka 9092

# Запуск supervisord
exec supervisord -c /app/supervisord.conf
