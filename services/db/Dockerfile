# services/db/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chmod +x /app/wait-for-services.sh

CMD ["/app/wait-for-services.sh", "bash", "-c", "python main.py && python process_messages.py"]
