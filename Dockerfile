FROM python:3.10-slim

# Установка зависимостей
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование папки tg
COPY tg /tg

# Копирование файла сессии
COPY session_name.session .

# Установить рабочую директорию
WORKDIR /tg/tgUserBot

# Запуск бота
CMD ["python3", "main.py"]
