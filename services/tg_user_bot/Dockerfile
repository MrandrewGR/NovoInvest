FROM python:3.11-slim

# Установите рабочую директорию
WORKDIR /app

# Скопируйте и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте приложение
COPY app/ ./app

# Создайте директории для логов и медиа
RUN mkdir -p /app/logs/chat /app/logs/channel /app/logs/kafka /app/logs/utils /app/logs/userbot /app/media

# Установите права доступа
RUN chmod -R 755 /app/logs /app/media

# Скопируйте wait-for-it.sh в /usr/local/bin и сделайте его исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Определите команду запуска с использованием wait-for-it.sh
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
