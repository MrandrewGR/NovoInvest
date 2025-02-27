# services/tg_ubot/Dockerfile

ARG CACHEBUST=1
ARG TG_UBOT_COMMIT_HASH=main

FROM python:3.11-slim

# Показываем переданные build-аргументы (для отладки)
RUN echo "Cache bust: ${CACHEBUST}" \
 && echo "Using tg_ubot commit: ${TG_UBOT_COMMIT_HASH}"

# Установка окружения, необходимых пакетов
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    git \
 && rm -rf /var/lib/apt/lists/*

# Часовой пояс
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем и устанавливаем Python-зависимости (если что-то нужно локально)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем внешнюю библиотеку tg_ubot из GitHub (конкретный коммит/ветку main)
RUN pip install --no-cache-dir --upgrade "git+https://github.com/MrandrewGR/tg_ubot.git"

# Создаём директории для логов, данных и т.д.
RUN mkdir -p /app/data /app/logs
RUN chmod -R 755 /app/data /app/logs

# Копируем скрипт ожидания сервисов (можно оставить как есть)
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# По умолчанию запускаем скрипт wait-for-it.sh, чтобы дождаться Kafka, затем
# запускаем основной модуль tg_ubot (т.к. теперь он есть в установленной либе).
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "tg_ubot.app.main"]
