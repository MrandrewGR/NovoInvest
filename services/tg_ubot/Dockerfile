# Dockerfile для сервиса tg_ubot

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Устанавливаем tzdata, чтобы иметь локальные файлы для часовых поясов
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем весь проект в контейнер
COPY . /app

# Устанавливаем библиотеку mirco_services_data_management из GitHub
RUN pip install --no-cache-dir git+https://github.com/MrandrewGR/mirco_services_data_management.git

# Затем устанавливаем остальные зависимости
RUN pip install --no-cache-dir -r services/tg_ubot/requirements.txt

# Создаем нужные директории
RUN mkdir -p /app/logs /app/data /app/media

# Скрипт ожидания Kafka (или других сервисов) при запуске
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
