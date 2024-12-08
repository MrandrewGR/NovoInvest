import json
import os
import random
import time
import logging
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Настройка логирования
logging.basicConfig(
    filename='scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Константы
URLS_JSON_PATH = 'tg/tgUserBot/extracted_links.json'  # Путь к вашему JSON файлу со списком URL
HTML_SAVE_DIR = 'downloaded_html'
PARSED_SAVE_DIR = 'parsed_data'
LOG_FILE = 'scraper.log'

# Создание директорий для сохранения, если они не существуют
os.makedirs(HTML_SAVE_DIR, exist_ok=True)
os.makedirs(PARSED_SAVE_DIR, exist_ok=True)

# Функция для генерации безопасного имени файла из URL
def generate_filename(url):
    parsed_url = urlparse(url)
    filename = parsed_url.path.replace('/', '_').strip('_')
    if not filename:
        filename = 'root'
    # Ограничение длины имени файла
    return f"{filename[:150]}.html"

# Загрузка списка URL из JSON файла
try:
    with open(URLS_JSON_PATH, 'r', encoding='utf-8') as f:
        urls = json.load(f)
    logging.info(f"Загружено {len(urls)} URL из {URLS_JSON_PATH}")
except Exception as e:
    logging.error(f"Ошибка при загрузке URL из JSON: {e}")
    raise

# Выбор случайных 1/10 URL
sample_size = max(1, len(urls) // 10)
sample_urls = random.sample(urls, sample_size)
logging.info(f"Выбрано {len(sample_urls)} случайных URL для обработки")

# Настройка заголовков для имитации реального браузера
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/112.0.0.0 Safari/537.36'
}

# Функция для загрузки HTML с обработкой ошибок и задержками
def fetch_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.text
        elif response.status_code == 403:
            logging.warning(f"Получен 403 для URL: {url}. Пауза на 5.5 минут.")
            time.sleep(5.5 * 60)  # Пауза 5.5 минут
            # После паузы можно попробовать снова или вернуть None
            return None
        else:
            logging.error(f"Неожиданный статус-код {response.status_code} для URL: {url}")
            return None
    except requests.RequestException as e:
        logging.error(f"Запрос к URL {url} завершился ошибкой: {e}")
        return None

# Функция для парсинга HTML и извлечения таблиц
def parse_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')

    extracted_tables = []

    for tab in tables:
        # Пытаемся найти caption
        caption_tag = tab.find('caption')
        caption = caption_tag.get_text(strip=True) if caption_tag else None

        # Пытаемся определить заголовки столбцов:
        headers = []
        thead = tab.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th','td'])]
        else:
            # Если нет thead, берем первую tr
            first_row = tab.find('tr')
            if first_row:
                headers = [th.get_text(strip=True) for th in first_row.find_all(['th','td'])]

        # Извлекаем все остальные строки
        rows = []
        all_tr = tab.find_all('tr')
        # Пропускаем первую строку если она заголовочная
        data_rows = all_tr[1:] if len(all_tr) > 1 else []

        for r in data_rows:
            cells = [td.get_text(strip=True) for td in r.find_all(['td','th'])]
            rows.append(cells)

        extracted_tables.append({
            "caption": caption,
            "headers": headers,
            "rows": rows,
        })

    return extracted_tables

# Основной цикл обработки URL
for url in tqdm(sample_urls, desc="Обработка URL"):
    filename = generate_filename(url)
    html_path = os.path.join(HTML_SAVE_DIR, filename)
    parsed_path = os.path.join(PARSED_SAVE_DIR, f"{filename}.json")

    # Проверка, был ли уже загружен этот URL
    if os.path.exists(parsed_path):
        logging.info(f"Пропуск уже обработанного URL: {url}")
        continue

    html_content = fetch_html(url)
    if html_content:
        try:
            # Сохранение HTML на диск
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Сохранен HTML для URL: {url} в файл {html_path}")

            # Парсинг HTML
            parsed_data = parse_html(html_content)

            # Сохранение распарсенных данных
            with open(parsed_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "url": url,
                    "parsed_tables": parsed_data
                }, f, ensure_ascii=False, indent=4)
            logging.info(f"Сохранены распарсенные данные для URL: {url} в файл {parsed_path}")

        except Exception as e:
            logging.error(f"Ошибка при обработке URL {url}: {e}")
    else:
        logging.warning(f"Не удалось получить контент для URL: {url}")

    # Задержка между запросами (например, от 1 до 3 секунд)
    delay = random.uniform(4, 12)
    time.sleep(delay)

logging.info("Завершение обработки всех URL")