# File: ./services/tg_file_bot/Dockerfile

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install netcat and curl
RUN apt-get update && apt-get install -y netcat curl && rm -rf /var/lib/apt/lists/*

# Download wait-for-it.sh and make it executable
RUN curl -o /usr/local/bin/wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod +x /usr/local/bin/wait-for-it.sh

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app

# Define the startup command
CMD ["wait-for-it.sh", "kafka:9092", "--timeout=60", "--strict", "--", "python", "-m", "app.main"]
