import requests
from bs4 import BeautifulSoup
import json
import re  # Импортируем модуль для работы с регулярными выражениями

def parse_corporate_action_info(url):
    response = requests.get(url)
    response.raise_for_status()  # Проверка на успешный запрос
    soup = BeautifulSoup(response.text, 'html.parser')

    # Извлечение заголовка
    title_element = soup.find('h1', class_='disc-message-header')
    title_text = title_element.get_text(strip=True) if title_element else None

    # Debug: вывод заголовка
    print(f"Заголовок: {title_text}")

    # Функция для извлечения ИНН из заголовка
    def extract_inn(title):
        match = re.search(r'ИНН\s*(\d{10}|\d{12})', title)
        if match:
            return match.group(1)
        return None

    # Функция для извлечения ISIN из заголовка
    def extract_isin(title):
        # Исправленное регулярное выражение: две буквы + десять буквенно-цифровых символов
        match = re.search(r'\bISIN\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{10})\b', title, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    inn = extract_inn(title_text) if title_text else None
    isin = extract_isin(title_text) if title_text else None

    # Debug: вывод извлечённых ИНН и ISIN
    print(f"Извлечённый ИНН: {inn}")
    print(f"Извлечённый ISIN: {isin}")

    # Функция для парсинга таблицы по заголовку
    def parse_table_by_title(soup, title_text):
        # Находим все таблицы на странице
        tables = soup.find_all('table')
        for table in tables:
            thead = table.find('thead')
            if thead and title_text in thead.get_text():
                # Парсим тело таблицы
                rows = table.find('tbody').find_all('tr', recursive=False)
                if not rows:
                    return None  # Если нет строк, возвращаем None

                # Определим число столбцов
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
                    return data
                else:
                    # Формат с несколькими столбцами. Например "Информация о ценных бумагах".
                    # Возьмем заголовки из thead
                    headers_rows = thead.find_all('tr')
                    headers = []
                    for header_row in headers_rows:
                        headers = [th.get_text(strip=True) for th in header_row.find_all('th')]

                    # Если заголовков нет, пропускаем таблицу
                    if not headers:
                        continue

                    # В итоге data будет списком словарей, соответствующих строкам
                    data_list = []
                    for row in rows:
                        cells = row.find_all('td', recursive=False)
                        row_data = {}
                        for h, c in zip(headers, cells):
                            row_data[h] = c.get_text(strip=True)
                        data_list.append(row_data)
                    return data_list
        return None

    # Парсим все необходимые таблицы
    corp_actions = parse_table_by_title(soup, "Реквизиты корпоративного действия")
    securities_info = parse_table_by_title(soup, "Информация о ценных бумагах")
    details = parse_table_by_title(soup, "Детали корпоративного действия")
    income_info = parse_table_by_title(soup, "Информация о выплате дохода")

    return {
        "Заголовок": title_text,
        "ИНН": inn,
        "ISIN": isin,
        "Реквизиты корпоративного действия": corp_actions,
        "Информация о ценных бумагах": securities_info,
        "Детали корпоративного действия": details,
        "Информация о выплате дохода": income_info
    }

# Пример использования:
if __name__ == "__main__":
    url = "https://nsddata.ru/ru/news/view/1132181"  # или любая страница со схожей структурой
    result = parse_corporate_action_info(url)

    # Сохраняем результаты в файл JSON
    with open('results.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    print("Результаты успешно сохранены в файл results.json")