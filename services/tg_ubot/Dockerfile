# File: services/tg_ubot/Dockerfile

ARG CACHEBUST=1

FROM python:3.11-slim

# Просто выводим build-аргумент (необязательно)
RUN echo "Cache bust: ${CACHEBUST}"

# Настраиваем окружение, устанавливаем нужные пакеты
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    git \
 && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем и ставим зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем скрипт ожидания Kafka
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Копируем исходный код так, чтобы пакет назывался "app"
COPY app/ /app/app/

# Запускаем бота, дождавшись Kafka на порту 9092.
# Обратите внимание, что теперь используем "python -m app.main"
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
