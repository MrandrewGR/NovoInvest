# File location: services/tg_user_bot/Dockerfile

FROM python:3.11-slim

# Установите рабочую директорию
WORKDIR /app

# Скопируйте и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте приложение
COPY app/ ./app

# Скопируйте wait-for-it.sh в /usr/local/bin и сделайте его исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Установите переменные окружения (опционально, если необходимо)
# ENV SOME_ENV_VAR=some_value

# Определите команду запуска с использованием wait-for-it.sh
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
