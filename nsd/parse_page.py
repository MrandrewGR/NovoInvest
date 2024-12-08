import json
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import os
import re
from datetime import datetime, timedelta
import heapq

# Конфигурация основного логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parse_page.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Создание отдельного логгера для ошибок
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)

error_handler = logging.FileHandler('error.log', mode='a', encoding='utf-8')
error_handler.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(formatter)

error_logger.addHandler(error_handler)

# Определение заголовков для имитации браузера
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/112.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# Инициализация сессии
session = requests.Session()
session.headers.update(HEADERS)

def parse_table(table):
    """
    Парсит таблицу и возвращает название таблицы, заголовки и данные.
    Поддерживаются разные паттерны для извлечения названия таблицы.
    """
    # Попытка найти название таблицы
    table_title = "Без названия"

    # 1. Проверка наличия <caption>
    caption = table.find('caption')
    if caption and caption.get_text(strip=True):
        table_title = caption.get_text(strip=True)
    else:
        # 2. Проверка заголовков в <th> с colspan
        thead = table.find('thead')
        if thead:
            # Ищем строку с заголовком таблицы
            header_row = thead.find('tr')
            if header_row:
                th_colspan = header_row.find('th', colspan=True)
                if th_colspan and th_colspan.get_text(strip=True):
                    table_title = th_colspan.get_text(strip=True)
        else:
            # 3. Проверка первых строк таблицы на наличие <p><strong>
            first_tr = table.find('tr')
            if first_tr:
                p_tag = first_tr.find('p')
                if p_tag:
                    strong_tag = p_tag.find('strong')
                    if strong_tag and strong_tag.get_text(strip=True):
                        table_title = strong_tag.get_text(strip=True)

    # Извлечение заголовков столбцов
    headers = []
    thead = table.find('thead')
    if thead:
        header_rows = thead.find_all('tr')
        for row in header_rows:
            th_tags = row.find_all('th')
            for th in th_tags:
                header_text = th.get_text(strip=True)
                if header_text:
                    headers.append(header_text)
    else:
        # Попытка извлечь заголовки из первого ряда таблицы
        first_tr = table.find('tr')
        if first_tr:
            th_tags = first_tr.find_all('th')
            if th_tags:
                for th in th_tags:
                    header_text = th.get_text(strip=True)
                    if header_text:
                        headers.append(header_text)

    # Если заголовки не найдены в <thead> или <th>, попробуем найти их в первом <tr> с <td>
    if not headers:
        first_tr = table.find('tr')
        if first_tr:
            th_tags = first_tr.find_all(['th', 'td'])
            for th in th_tags:
                header_text = th.get_text(strip=True)
                if header_text:
                    headers.append(header_text)

    # Извлечение данных таблицы
    data = []
    tbody = table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        # Если нет <tbody>, ищем все <tr> кроме первого, если оно содержит заголовки
        all_tr = table.find_all('tr')
        if headers and len(all_tr) > 1:
            rows = all_tr[1:]
        else:
            rows = all_tr

    for row in rows:
        row_data = {}
        cells = row.find_all(['td', 'th'])
        for idx, cell in enumerate(cells):
            cell_text = cell.get_text(strip=True)
            if headers and idx < len(headers):
                row_data[headers[idx]] = cell_text
            else:
                # Если заголовки отсутствуют или ячейка превышает количество заголовков
                row_data[f"Column {idx+1}"] = cell_text
        if row_data:
            data.append(row_data)

    return {
        "title": table_title,
        "headers": headers,
        "data": data
    }

def parse_page(url, max_retries=3):
    """
    Основная функция для парсинга страницы.
    Возвращает кортеж (успешно_разобран, список_таблиц, причина_неудачи).
    """
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Поиск всех таблиц на странице
            tables = soup.find_all('table')
            if tables:
                parsed_tables = [parse_table(table) for table in tables]
                table_titles = [t['title'] for t in parsed_tables]
                logging.info(f"Успешно разобран {url} - найдено {len(tables)} таблиц: {', '.join(table_titles)}")
                return True, parsed_tables, None
            else:
                logging.warning(f"Таблицы не найдены на странице {url}.")
                return False, [], 'no_table'
        except requests.exceptions.HTTPError as http_err:
            error_logger.error(f"HTTP ошибка для {url}: {http_err}")
            if response.status_code == 403:
                # Специфическая обработка 403 ошибки
                error_logger.error(f"Доступ запрещён для {url}. Возможно, IP заблокирован.")
                return False, [], '403'
            else:
                return False, [], 'http_error'
        except requests.exceptions.RequestException as req_err:
            error_logger.error(f"Ошибка запроса для {url}: {req_err}")
            failure_reason = 'request_exception'
        except Exception as err:
            error_logger.error(f"Неожиданная ошибка для {url}: {err}")
            failure_reason = 'unexpected_error'

        retries += 1
        logging.info(f"Повторная попытка {retries}/{max_retries} для {url}")
        time.sleep(random.uniform(5, 10))  # Увеличенная задержка между попытками

    return False, [], failure_reason

def load_urls(file_path):
    """
    Загружает список URL из JSON-файла.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = json.load(f)
        logging.info(f"Загружено {len(urls)} URL из {file_path}")
        return urls
    except json.JSONDecodeError as json_err:
        error_logger.error(f"Ошибка декодирования JSON: {json_err}")
    except FileNotFoundError:
        error_logger.error(f"Файл не найден: {file_path}")
    except Exception as err:
        error_logger.error(f"Ошибка при загрузке URL: {err}")
    return []

def save_progress(all_results, output_file):
    """
    Сохраняет собранные результаты в JSON-файл.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=4)
        logging.info(f"Результаты успешно сохранены в {output_file}")
    except Exception as err:
        error_logger.error(f"Ошибка при сохранении результатов: {err}")

def main():
    """
    Основная функция для выполнения парсинга всех URL.
    """
    urls = load_urls(EXTRACTED_LINKS_FILE)
    if not urls:
        logging.error("Нет URL для обработки.")
        return

    # Выбираем каждый 5-й URL
    filtered_urls = urls[::5]
    logging.info(f"Отобрано {len(filtered_urls)} URL для обработки.")

    all_results = {}
    processed_count = 0
    total = len(filtered_urls)

    # Очередь для повторной проверки (минимальная куча по времени повторной попытки)
    retry_heap = []
    # Словарь для отслеживания количества попыток
    retry_counts = {}

    # Проверка наличия существующего файла с результатами для продолжения
    if os.path.exists(ALL_RESULTS_FILE):
        try:
            with open(ALL_RESULTS_FILE, 'r', encoding='utf-8') as f:
                all_results = json.load(f)
            processed_count = len(all_results)
            logging.info(f"Загружено {processed_count} уже обработанных URL из {ALL_RESULTS_FILE}")
        except Exception as e:
            error_logger.error(f"Не удалось загрузить существующие результаты: {e}")

    # Обработка URL с учетом уже обработанных
    for idx, u in enumerate(filtered_urls, start=1):
        if u in all_results:
            continue  # Пропускаем уже обработанные URL

        print(f"Обработка {processed_count + 1}/{total}: {u}")
        success, parsed_tables, failure_reason = parse_page(u)

        if success:
            all_results[u] = {
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "tables": parsed_tables
            }
        else:
            if failure_reason == '403':
                logging.error(f"URL {u} вернул 403 ошибку. Добавление в очередь для повторной попытки через 5 минут.")
                retry_time = datetime.now() + timedelta(minutes=5)
                heapq.heappush(retry_heap, (retry_time, u))
                retry_counts[u] = retry_counts.get(u, 0) + 1
            elif failure_reason == 'no_table':
                logging.warning(f"URL {u} не содержит таблиц. Добавление в очередь для повторной попытки через 5 минут.")
                retry_time = datetime.now() + timedelta(minutes=5)
                heapq.heappush(retry_heap, (retry_time, u))
                retry_counts[u] = retry_counts.get(u, 0) + 1
            else:
                logging.warning(f"URL {u} не удалось обработать по причине: {failure_reason}. Пропуск или добавление в очередь при необходимости.")

        processed_count += 1  # Увеличиваем счётчик независимо от успешности

        # Сохранение прогресса каждые 100 обработанных URL
        if processed_count % 100 == 0:
            save_progress(all_results, ALL_RESULTS_FILE)
            logging.info(f"Сохранено {processed_count} результатов.")

        # Введение случайной задержки
        time.sleep(random.uniform(5, 10))

    # Обработка очереди для повторной проверки
    while retry_heap:
        current_time = datetime.now()
        retry_time, u = heapq.heappop(retry_heap)

        if current_time < retry_time:
            # Ждём до момента, когда можно повторить попытку
            wait_seconds = (retry_time - current_time).total_seconds()
            logging.info(f"Ожидание {wait_seconds:.0f} секунд до повторной попытки для {u}")
            time.sleep(wait_seconds)

        current_retry = retry_counts.get(u, 0)

        if current_retry > MAX_RETRIES:
            logging.error(f"URL {u} превысил максимальное количество попыток ({MAX_RETRIES}). Пропуск.")
            continue

        print(f"Повторная обработка {processed_count + 1}/{total} (Попытка {current_retry}/{MAX_RETRIES}): {u}")
        success, parsed_tables, failure_reason = parse_page(u)

        if success:
            all_results[u] = {
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "tables": parsed_tables
            }
            logging.info(f"Успешно разобран {u} при повторной попытке.")
        else:
            if failure_reason in ['403', 'no_table']:
                if current_retry < MAX_RETRIES:
                    logging.warning(f"URL {u} не удалось обработать при повторной попытке по причине: {failure_reason}. Добавление в очередь для повторной попытки через 5 минут.")
                    retry_time = datetime.now() + timedelta(minutes=5)
                    heapq.heappush(retry_heap, (retry_time, u))
                    retry_counts[u] += 1
                else:
                    logging.error(f"URL {u} достиг максимального количества попыток ({MAX_RETRIES}). Пропуск.")
            else:
                logging.warning(f"URL {u} не удалось обработать при повторной попытке по неизвестной причине: {failure_reason}. Пропуск.")

        processed_count += 1  # Увеличиваем счётчик независимо от успешности

        # Сохранение прогресса каждые 100 обработанных URL
        if processed_count % 100 == 0:
            save_progress(all_results, ALL_RESULTS_FILE)
            logging.info(f"Сохранено {processed_count} результатов.")

        # Введение случайной задержки
        time.sleep(random.uniform(5, 10))

    # Сохранение окончательных результатов
    save_progress(all_results, ALL_RESULTS_FILE)
    print("Обработка завершена. Проверьте 'all_results.json' для вывода.")

# Константы для путей и других конфигураций
EXTRACTED_LINKS_FILE = '../tg/tgUserBot/extracted_links.json'
ALL_RESULTS_FILE = 'all_results.json'
MAX_RETRIES = 3  # Максимальное количество повторных попыток для одного URL

if __name__ == "__main__":
    main()
