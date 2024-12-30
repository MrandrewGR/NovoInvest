# File location: ./services/file_processor/wait-for-services.sh
#!/usr/bin/env bash

# Ждём запуска Kafka
echo "Waiting for Kafka to be ready..."
/wait-for-it.sh kafka:9092 --timeout=60 --strict -- echo "Kafka is up"
exec "$@"
