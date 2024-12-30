# File: ./services/tg_file_bot/Dockerfile

FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install netcat-openbsd and curl
RUN apt-get update && \
    apt-get install -y netcat-openbsd curl && \
    rm -rf /var/lib/apt/lists/*

# Copy wait-for-it.sh and make it executable
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app

# Define the startup command using wait-for-it.sh
CMD ["wait-for-it.sh", "kafka:9092", "--timeout=60", "--strict", "--", "python", "-m", "app.main"]
