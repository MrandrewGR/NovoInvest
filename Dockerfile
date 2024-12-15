FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY userbot ./userbot
COPY media ./media
COPY logs ./logs
COPY .env .env

CMD ["python", "-m", "userbot.main"]
