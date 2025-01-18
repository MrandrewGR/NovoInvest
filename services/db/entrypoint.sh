#!/bin/bash
set -e

# Ожидание запуска зависимых сервисов
/app/wait-for-services.sh

# Запуск supervisord
exec supervisord -c /app/supervisord.conf
