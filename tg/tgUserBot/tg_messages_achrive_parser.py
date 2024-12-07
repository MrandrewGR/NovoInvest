import os
import glob
import csv
import json
import re

def extract_links_from_text(text):
    """
    Извлекает все URL из текста с использованием регулярных выражений.
    """
    # Регулярное выражение для поиска URL
    url_pattern = re.compile(
        r'https?://[^\s"\'<>]+',
        re.IGNORECASE
    )
    return url_pattern.findall(text)

def clean_url(url):
    """
    Очищает URL от возможных лишних символов.
    """
    # Удаляем возможные символы после валидного URL
    # Например, удаляем закрывающую кавычку или угловую скобку
    if url.endswith(('"', "'", '>', '/')):
        url = url.rstrip('"\'>/')
    # Дополнительная проверка: URL должен начинаться с http:// или https://
    if url.startswith(('http://', 'https://')):
        return url
    return None

def save_links_csv(links, filename):
    """
    Сохраняет список ссылок в CSV-файл.
    """
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['URL'])  # Заголовок
            for link in sorted(links):
                writer.writerow([link])
        print(f"Ссылки успешно сохранены в '{filename}'.")
    except Exception as e:
        print(f"Ошибка при записи в CSV файл '{filename}': {e}")

def save_links_json(links, filename):
    """
    Сохраняет список ссылок в JSON-файл.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(list(sorted(links)), jsonfile, ensure_ascii=False, indent=4)
        print(f"Ссылки успешно сохранены в '{filename}'.")
    except Exception as e:
        print(f"Ошибка при записи в JSON файл '{filename}': {e}")

def main():
    """
    Основная функция для извлечения ссылок и сохранения их в файлы.
    """
    # Жёстко заданный путь к папке с HTML-файлами
    folder_path = '/Users/andreigrishin/Downloads/Telegram Desktop/ChatExport_2024-12-06'

    # Проверка существования папки
    if not os.path.isdir(folder_path):
        print(f"Указанная папка не существует: {folder_path}")
        return

    # Поиск всех .html файлов в указанной папке и её подкаталогах
    html_files = glob.glob(os.path.join(folder_path, '**', '*.html'), recursive=True)

    if not html_files:
        print(f"В папке '{folder_path}' не найдено HTML-файлов.")
        return

    print(f"Найдено {len(html_files)} HTML-файлов для обработки.")

    all_links = set()

    for file_path in html_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                links = extract_links_from_text(content)
                # Фильтрация и очистка ссылок
                for link in links:
                    cleaned_link = clean_url(link)
                    if cleaned_link:
                        all_links.add(cleaned_link)
        except Exception as e:
            print(f"Ошибка при чтении файла '{file_path}': {e}")

    if not all_links:
        print("Не найдено ни одной ссылки.")
        return

    # Сохранение ссылок в CSV и JSON
    save_links_csv(all_links, 'extracted_links.csv')
    save_links_json(all_links, 'extracted_links.json')

    print(f"\nВсего найдено {len(all_links)} уникальных ссылок.")

if __name__ == "__main__":
    main()