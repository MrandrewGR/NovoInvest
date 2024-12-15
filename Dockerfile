FROM python:3.10-slim

# Установка зависимостей
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода бота
COPY userbot/ ./userbot/

# Копирование файла сессии
COPY session_name.session .

# Запуск бота
CMD ["python3", "userbot/main.py"]
