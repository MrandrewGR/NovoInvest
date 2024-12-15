# Используем базовый образ Python
FROM python:3.10-slim

# Установка зависимостей системы
RUN apt-get update && apt-get install -y netcat-openbsd

# Копирование требований и установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование папки tg
COPY tg /tg

# Установить рабочую директорию
WORKDIR /tg/tgUserBot

# Запуск бота
CMD ["./wait-for-kafka.sh", "kafka", "9092", "python3", "main.py"]
