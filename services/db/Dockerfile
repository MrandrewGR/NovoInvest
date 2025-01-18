# services/db/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей и supervisor
RUN apt-get update \
    && apt-get install -y netcat-openbsd supervisor \
    && rm -rf /var/lib/apt/lists/*

# Копирование и установка Python-зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всего кода приложения
COPY . /app

# Копирование конфигурации supervisord
COPY supervisord.conf /app/supervisord.conf

# Копирование entrypoint.sh и установка прав
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Копирование wait-for-services.sh и установка прав
RUN chmod +x /app/wait-for-services.sh

# Установка точки входа
ENTRYPOINT ["/app/entrypoint.sh"]
