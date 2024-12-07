import json
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import os
import re
from datetime import datetime

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
    Парсит таблицу и возвращает название таблицы и её данные.
    """
    thead = table.find('thead')
    tbody = table.find('tbody')
    if not thead or not tbody:
        return None, None

    # Извлечение заголовка таблицы
    title_row = thead.find_all('tr')[0]
    title_text = title_row.get_text(strip=True)

    rows = tbody.find_all('tr', recursive=False)
    if not rows:
        return title_text, []

    first_row_cells = rows[0].find_all('td', recursive=False)
    if len(first_row_cells) == 2:
        # Формат ключ-значение
        data = {}
        for row in rows:
            cells = row.find_all('td', recursive=False)
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                data[key] = value
        return title_text, data
    else:
        # Формат таблицы со строками и столбцами
        header_rows = thead.find_all('tr')
        headers = []
        for header_row in header_rows:
            ths = header_row.find_all('th')
            # Если в заголовке есть colspan, нужно аккуратно их обработать,
            # но в данном случае предполагаем прямолинейную структуру
            if ths:
                headers = [th.get_text(strip=True) for th in ths if th.get_text(strip=True)]

        if not headers:
            return title_text, []  # Пропускаем таблицу без явных заголовков столбцов

        data = []
        for row in rows:
            cells = row.find_all('td', recursive=False)
            # Пропускаем строки, если количество столбцов не совпадает
            if len(cells) != len(headers):
                continue
            row_data = {}
            for h, c in zip(headers, cells):
                row_data[h] = c.get_text(strip=True)
            data.append(row_data)

        return title_text, data


def extract_inn(title):
    """
    Извлекает ИНН из заголовка.
    """
    match = re.search(r'ИНН\s*(\d{10}|\d{12})', title)
    if match:
        return match.group(1)
    return None


def extract_isin(title):
    """
    Извлекает ISIN из заголовка.
    """
    match = re.search(r'\bISIN\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{10})\b', title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def parse_all_tables(soup):
    """
    Парсит все таблицы на странице и возвращает словарь с данными.
    Ключ - заголовок таблицы, значение - данные таблицы.
    """
    specific_tables = {}
    tables = soup.find_all('table')
    for table in tables:
        t_title, t_data = parse_table(table)
        if t_title and t_data is not None:
            specific_tables[t_title] = t_data
    return specific_tables


def parse_corporate_action_info(soup):
    """
    Извлекает заголовок, ИНН, ISIN и парсит все таблицы.
    """
    # Извлечение заголовка
    title_element = soup.find('h1', class_='disc-message-header')
    title_text = title_element.get_text(strip=True) if title_element else None

    # Извлечение ИНН и ISIN
    inn = extract_inn(title_text) if title_text else None
    isin = extract_isin(title_text) if title_text else None

    # Парсинг всех таблиц
    all_tables = parse_all_tables(soup)

    return {
        "Заголовок": title_text,
        "ИНН": inn,
        "ISIN": isin,
        **all_tables
    }


def parse_page(url, max_retries=3):
    """
    Основная функция для парсинга страницы.
    """
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Парсинг специфических данных
            specific_data = parse_corporate_action_info(soup)

            if not specific_data:
                logging.warning(f"Не удалось извлечь специфические данные из {url}")
                return None

            logging.info(f"Успешно разобран {url}")
            return specific_data
        except requests.exceptions.HTTPError as http_err:
            error_logger.error(f"HTTP ошибка для {url}: {http_err}")
            if response.status_code == 403:
                # Специфическая обработка 403 ошибки
                error_logger.error(f"Доступ запрещён для {url}. Возможно, IP заблокирован.")
                break  # Прекратить попытки для 403 ошибки
        except requests.exceptions.RequestException as req_err:
            error_logger.error(f"Ошибка запроса для {url}: {req_err}")
        except Exception as err:
            error_logger.error(f"Неожиданная ошибка для {url}: {err}")

        retries += 1
        logging.info(f"Повторная попытка {retries}/{max_retries} для {url}")
        time.sleep(random.uniform(5, 10))  # Увеличенная задержка между попытками

    return None


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


def save_progress(all_results, output_file='all_results.json'):
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
    urls = load_urls('extracted_links.json')
    if not urls:
        logging.error("Нет URL для обработки.")
        return

    # Выбираем каждый 5-й URL
    filtered_urls = urls[::5]
    logging.info(f"Отобрано {len(filtered_urls)} URL для обработки.")

    all_results = {}
    processed_count = 0
    total = len(filtered_urls)

    # Проверка наличия существующего файла с результатами для продолжения
    if os.path.exists('all_results.json'):
        try:
            with open('all_results.json', 'r', encoding='utf-8') as f:
                all_results = json.load(f)
            processed_count = len(all_results)
            logging.info(f"Загружено {processed_count} уже обработанных URL из all_results.json")
        except Exception as e:
            error_logger.error(f"Не удалось загрузить существующие результаты: {e}")

    # Обработка URL с учетом уже обработанных
    for idx, u in enumerate(filtered_urls, start=1):
        if u in all_results:
            continue  # Пропускаем уже обработанные URL

        print(f"Обработка {processed_count + 1}/{total}: {u}")
        result = parse_page(u)
        processed_count += 1  # Увеличиваем счётчик независимо от успешности

        if result:
            all_results[u] = result

        # Сохранение прогресса каждые 100 обработанных URL
        if processed_count % 100 == 0:
            save_progress(all_results)
            logging.info(f"Сохранено {processed_count} результатов.")

        # Введение увеличенной случайной задержки
        time.sleep(random.uniform(5, 10))

    # Сохранение окончательных результатов
    save_progress(all_results)
    print("Обработка завершена. Проверьте 'all_results.json' для вывода.")


if __name__ == "__main__":
    main()
