# Dockerfile

# Базовый образ Python
FROM python:3.11-slim

# Установка зависимостей системы
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории
WORKDIR /app

# Копирование зависимостей
COPY requirements.txt .

# Установка зависимостей Python
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY app/ ./app

# Создание директорий для логов и медиа
RUN mkdir -p logs/chat logs/channel logs/kafka_producer logs/utils media

# Указание точки входа
CMD ["python", "-m", "app.main"]
