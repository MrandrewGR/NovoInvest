# File location: services/tg_ubot/Dockerfile
FROM python:3.11-slim

# Отключаем интерактивный режим, чтобы tzdata не запрашивала часовой пояс в консоли
ENV DEBIAN_FRONTEND=noninteractive

# Устанавливаем tzdata, чтобы иметь локальные файлы для часовых поясов
RUN apt-get update && apt-get install -y --no-install-recommends tzdata

# Настраиваем часовой пояс на московский
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Установите рабочую директорию
WORKDIR /app

# Скопируйте и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте приложение
COPY app/ ./app

# Создайте директории для логов и медиа
RUN mkdir -p /app/logs/chat /app/logs/channel /app/logs/kafka /app/logs/utils /app/logs/userbot /app/media

# Установите права доступа
RUN chmod -R 755 /app/logs /app/media

# Скопируйте wait-for-it.sh в /usr/local/bin и сделайте его исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Определите команду запуска с использованием wait-for-it.sh
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
