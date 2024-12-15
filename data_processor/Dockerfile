# data_processor/Dockerfile

# Используем официальный образ Python
FROM python:3.10-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y gcc libpq-dev

# Установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY app/ /app/app/
COPY alembic.ini /app/
COPY migrations/ /app/migrations/
COPY wait-for-kafka.sh /app/wait-for-kafka.sh
RUN chmod +x /app/wait-for-kafka.sh

WORKDIR /app

# Выполнение миграций и запуск приложения
CMD ["bash", "-c", "./wait-for-kafka.sh kafka && alembic upgrade head && python app/consumer.py"]
