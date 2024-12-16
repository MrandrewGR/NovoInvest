# data_processor/Dockerfile

# Используем официальный образ Python
FROM python:3.10-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y gcc libpq-dev netcat-openbsd

# Установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY . /app/
COPY alembic.ini /app/
COPY migrations/ /app/migrations/
COPY wait-for-services.sh /app/wait-for-services.sh
RUN chmod +x /app/wait-for-services.sh

WORKDIR /app

# Выполнение миграций и запуск приложения
CMD ["bash", "-c", "./wait-for-services.sh && alembic upgrade head && python consumer.py"]
