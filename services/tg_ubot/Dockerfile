FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Устанавливаем tzdata, чтобы иметь локальные файлы для часовых поясов
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем ВСЁ (включая ./utils/libs/mirco_services_data_management),
# чтобы можно было установить библиотеку локально
COPY . /app

# Сначала устанавливаем библиотеку из локальной папки
RUN pip install --no-cache-dir ./utils/libs/mirco_services_data_management

# Затем основные зависимости для tg_ubot
RUN pip install --no-cache-dir -r services/tg_ubot/requirements.txt

# Создаем нужные директории
RUN mkdir -p /app/logs /app/data /app/media

# Скрипт ожидания Kafka (или других сервисов) при запуске
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
