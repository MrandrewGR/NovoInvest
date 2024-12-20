FROM python:3.10-slim

# Установка зависимостей
RUN apt-get update && apt-get install -y gcc libpq-dev netcat-openbsd

# Установка рабочей директории
WORKDIR /app

# Установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всех файлов микросервиса
COPY . /app

# Установка переменной окружения PYTHONPATH
ENV PYTHONPATH=/app/app

# Запуск скрипта ожидания и выполнения миграций
CMD ["bash", "-c", "./wait-for-services.sh && alembic upgrade head && python app/consumer.py"]
