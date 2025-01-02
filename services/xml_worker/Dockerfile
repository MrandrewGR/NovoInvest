# File location: ./services/xml_worker/Dockerfile

FROM python:3.10-slim

WORKDIR /app

# Скопируем requirements.txt
COPY requirements.txt /app/

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY app /app/app

# Запускаем main.py
CMD ["python", "-m", "app.main"]
