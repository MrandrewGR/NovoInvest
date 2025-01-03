# File location: ./services/xml_worker/Dockerfile
#
# Обновленная версия, которая:
#  1) Устанавливает системные зависимости (build-essential), чтобы исключить ошибки совместимости
#  2) Обновляет pip/setuptools/wheel перед установкой зависимостей
#  3) Запускает Python в "unbuffered" режиме, чтобы логи были видны в docker logs

FROM python:3.10-slim

# Устанавливаем переменную окружения, чтобы Python не буферизовал вывод (видно в docker logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Установим системные зависимости, чтобы Pandas и Numpy могли собираться корректно
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Скопируем requirements.txt
COPY requirements.txt /app/

# Обновим pip, setuptools и wheel для устранения проблем совместимости
RUN pip install --no-cache-dir -U pip setuptools wheel

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY app /app/app

# Запуск приложения (главный скрипт)
CMD ["python", "-m", "app.main"]
