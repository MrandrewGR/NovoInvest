# File location: services/tg_standard_bot/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app

WORKDIR /app

CMD ["python", "-m", "app.main"]

