# File location: ./services/bot/Dockerfile

FROM python:3.11-slim

# Установите переменную окружения, чтобы Python не буферизовал вывод (видно в docker logs)
ENV PYTHONUNBUFFERED=1

# Установите рабочую директорию
WORKDIR /app

# Установите системные зависимости, включая netcat и curl для скрипта ожидания,
# а также build-essential для сборки некоторых Python-пакетов
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        netcat-openbsd \
        curl \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Скопируйте ваш собственный скрипт ожидания и сделайте его исполняемым
COPY wait-for-services.sh /usr/local/bin/wait-for-services.sh
RUN chmod +x /usr/local/bin/wait-for-services.sh

# Скопируйте requirements.txt и установите зависимости
COPY requirements.txt /app/
RUN pip install --no-cache-dir -U pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Скопируйте код приложения
COPY app/ ./app

# Определите команду запуска с использованием вашего скрипта ожидания
CMD ["wait-for-services.sh", "python", "-m", "app.main"]
