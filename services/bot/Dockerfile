# File location: ./services/file_processor/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Скопируем requirements.txt и установим зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем код приложения
COPY . .

# Разрешим выполнение скрипта ожидания
RUN chmod +x wait-for-services.sh

# Запускаем сначала wait-for-services.sh, затем Python
CMD ["./wait-for-services.sh", "python", "-m", "app.main"]
