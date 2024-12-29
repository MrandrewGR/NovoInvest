# services/db/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Копируем основной код (main.py, wait-for-services.sh и т.д.)
COPY . /app

# Делаем скрипт ожидания сервисов исполняемым
RUN chmod +x /app/wait-for-services.sh

# Запуск:
#  1) дождаться готовности postgres и kafka
#  2) запустить main.py
CMD ["/app/wait-for-services.sh", "python", "main.py"]
