# services/nsd_worker/Dockerfile

# Build arguments: adjust CACHEBUST to force rebuild manually; LIB_COMMIT_HASH is set via docker-compose or build command.
ARG CACHEBUST=1
ARG LIB_COMMIT_HASH=main

FROM python:3.11-slim

# Install necessary system packages including netcat-openbsd for nc utility
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    git \
    netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install external library from GitHub
RUN pip install --no-cache-dir --upgrade "git+https://github.com/MrandrewGR/mirco_services_data_management.git"
RUN pip show mirco_services_data_management || (echo "Package not installed!" && exit 1)
RUN python -c "import mirco_services_data_management; print('Package mirco_services_data_management is installed')"

# Copy application source code
COPY app/ ./app

# Create logs directory
RUN mkdir -p /app/logs && chmod -R 755 /app/logs

# Copy wait-for-services.sh script and make it executable
COPY wait-for-services.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Command to start the app, waiting for PostgreSQL and Kafka to become available
CMD ["wait-for-it.sh", "postgres:5432", "--timeout=60", "--", "wait-for-it.sh", "kafka:9092", "--timeout=60", "--", "python", "-m", "app.main"]
