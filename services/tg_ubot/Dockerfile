# services/tg_ubot/Dockerfile

# Build arguments: adjust CACHEBUST to force rebuild manually; LIB_COMMIT_HASH is set via docker-compose or build command.
ARG CACHEBUST=1
ARG LIB_COMMIT_HASH=main

FROM python:3.11-slim

# Use the build args to bust the cache and show the commit being used
RUN echo "Cache bust: ${CACHEBUST}" && echo "Using mirco_services_data_management commit: ${LIB_COMMIT_HASH}"

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends tzdata git && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the external library using the provided commit hash.
RUN pip install --no-cache-dir --upgrade "git+https://github.com/MrandrewGR/mirco_services_data_management.git"

# Verify installation
RUN pip show mirco_services_data_management || (echo "Package not installed!" && exit 1)
RUN python -c "import mirco_services_data_management; print('Package mirco_services_data_management is installed')"

# Copy the app source code
COPY app/ ./app

# Create directories for logs and media
RUN mkdir -p /app/logs/chat /app/logs/channel /app/logs/kafka /app/logs/utils /app/logs/userbot /app/media
RUN chmod -R 755 /app/logs /app/media

# Copy the wait-for-it.sh script and make it executable
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Command to start the app (waiting for Kafka)
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
