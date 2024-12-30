# File location: ./services/tg_file_bot/Dockerfile
FROM python:3.11-slim

# Установите рабочую директорию
WORKDIR /app

# Установите netcat для работы wait-for-it.sh
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Скопируйте wait-for-it.sh и сделайте его исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Скопируйте requirements.txt и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте код приложения
COPY app/ ./app

# Определите команду запуска с использованием wait-for-it.sh
CMD ["wait-for-it.sh", "kafka:9092", "--timeout=60", "--strict", "--", "python", "-m", "app.main"]
