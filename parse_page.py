import json
import requests
from bs4 import BeautifulSoup
import time
import random
import logging

# Configure logging
logging.basicConfig(
    filename='parse_page.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Define headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/112.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# Initialize a session
session = requests.Session()
session.headers.update(HEADERS)

def parse_table(table):
    thead = table.find('thead')
    tbody = table.find('tbody')
    if not thead or not tbody:
        return None, None

    # Предполагаем, что заголовок таблицы в первом tr из thead
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
    else:
        # Формат таблицы со строками и столбцами
        header_rows = thead.find_all('tr')
        header_cells = header_rows[-1].find_all('th', recursive=False)
        headers = [th.get_text(strip=True) for th in header_cells]

        data = []
        for row in rows:
            cells = row.find_all('td', recursive=False)
            if len(cells) == len(headers):
                row_data = {}
                for h, c in zip(headers, cells):
                    row_data[h] = c.get_text(strip=True)
                data.append(row_data)

    return title_text, data

def parse_page(url):
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        result = {}
        tables = soup.find_all('table')
        for table in tables:
            thead = table.find('thead')
            if thead:
                title, data = parse_table(table)
                if title and data is not None:
                    result[title] = data
        logging.info(f"Successfully parsed {url}")
        return result
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error for {url}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request exception for {url}: {req_err}")
    except Exception as err:
        logging.error(f"Unexpected error for {url}: {err}")
    return None

def load_urls(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = json.load(f)
        logging.info(f"Loaded {len(urls)} URLs from {file_path}")
        return urls
    except json.JSONDecodeError as json_err:
        logging.error(f"JSON decode error: {json_err}")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except Exception as err:
        logging.error(f"Error loading URLs: {err}")
    return []

def main():
    urls = load_urls('extracted_links.json')
    if not urls:
        logging.error("No URLs to process.")
        return

    all_results = {}
    for idx, u in enumerate(urls, start=1):
        print(f"Processing {idx}/{len(urls)}: {u}")
        result = parse_page(u)
        if result:
            all_results[u] = result
        # Introduce a random delay between 1 to 3 seconds
        time.sleep(random.uniform(1, 3))

    # Save the results to a JSON file
    try:
        with open('all_results.json', 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=4)
        logging.info("All results successfully saved to all_results.json")
    except Exception as err:
        logging.error(f"Error saving results: {err}")

    print("Processing complete. Check 'all_results.json' for the output.")

if __name__ == "__main__":
    main()