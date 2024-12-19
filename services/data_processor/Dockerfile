# File location: services/data_processor/Dockerfile

FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc libpq-dev netcat-openbsd

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic.ini /app/
COPY migrations /app/migrations/
COPY wait-for-services.sh /app/wait-for-services.sh
RUN chmod +x /app/wait-for-services.sh

COPY app /app/

CMD ["bash", "-c", "./wait-for-services.sh && alembic upgrade head && python consumer.py"]
