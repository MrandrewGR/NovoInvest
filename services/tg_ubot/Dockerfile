# File: services/tg_ubot/Dockerfile

ARG CACHEBUST=1
ARG TG_UBOT_COMMIT_HASH=main

FROM python:3.11-slim

# Показываем, какие ARG были переданы (необязательно, но полезно для отладки)
RUN echo "Cache bust: ${CACHEBUST}" \
 && echo "Using tg_ubot commit: ${TG_UBOT_COMMIT_HASH}"

# Установка окружения и необходимых системных пакетов
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    git \
 && rm -rf /var/lib/apt/lists/*

# Настройка часового пояса
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем и устанавливаем Python-зависимости (если что-то нужно локально)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# При необходимости устанавливаем внешний репозиторий (закомментировано):
# RUN pip install --no-cache-dir --upgrade "git+https://github.com/MrandrewGR/tg_ubot.git"

# Создаём каталоги для данных и логов; выставляем права
RUN mkdir -p /app/data /app/logs
RUN chmod -R 755 /app/data /app/logs

# **Ключевой момент**: копируем локальные исходники tg_ubot,
# чтобы в контейнере был доступен код из app/
COPY app/ /app/tg_ubot/app/

# Копируем скрипт ожидания сервиса (Kafka) и делаем исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# По умолчанию запускаем бота, дождавшись Kafka на порту 9092
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "tg_ubot.app.main"]
