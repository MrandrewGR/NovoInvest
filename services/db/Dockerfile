# services/db/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Установим зависимости
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Копируем всё содержимое (код, alembic.ini, migrations/, ...)
COPY . /app

# Делаем скрипт ожидания сервисов исполняемым
RUN chmod +x /app/wait-for-services.sh

# При старте:
# 1) Ждём Postgres, Kafka
# 2) Запускаем main.py (создаст базу, если нет)
# 3) alembic upgrade head (накатит миграции)
# 4) process_messages.py (основной код)
CMD ["/app/wait-for-services.sh", "bash", "-c", "python main.py && alembic upgrade head && python process_messages.py"]
