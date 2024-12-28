# File: services/data_processor/Dockerfile

FROM python:3.10-slim

# Установка зависимостей системы
RUN apt-get update && apt-get install -y gcc libpq-dev netcat-openbsd

# Установка рабочей директории
WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Копируем всё содержимое микросервиса
COPY . /app

# Делаем скрипт ожидания сервисов исполняемым
RUN chmod +x /app/wait-for-services.sh

# Установка переменной окружения PYTHONPATH (важно, чтобы /app/ было корнем)
ENV PYTHONPATH=/app

# Запуск: ждем зависимости, выполняем миграции и запускаем consumer
CMD ["bash", "-c", "./wait-for-services.sh && alembic upgrade head && python -m app.consumer"]
