# File location: ./services/bot/Dockerfile
FROM python:3.11-slim

# Установите рабочую директорию
WORKDIR /app

# Установите netcat и curl для работы скрипта ожидания
RUN apt-get update && \
    apt-get install -y netcat-openbsd curl && \
    rm -rf /var/lib/apt/lists/*

# Скопируйте ваш собственный скрипт ожидания и сделайте его исполняемым
COPY wait-for-services.sh /usr/local/bin/wait-for-services.sh
RUN chmod +x /usr/local/bin/wait-for-services.sh

# Скопируйте requirements.txt и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте код приложения
COPY app/ ./app

# Определите команду запуска с использованием вашего скрипта ожидания
CMD ["wait-for-services.sh", "python", "-m", "app.main"]
