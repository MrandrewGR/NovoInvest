import os

def is_hidden(filepath):
    """Проверяет, является ли файл или папка скрытым."""
    return os.path.basename(filepath).startswith('.')

def generate_tree(dir_path, file, prefix=""):
    """Рекурсивно обходит директорию и записывает структуру в файл, исключая скрытые файлы и папки."""
    try:
        items = sorted(os.listdir(dir_path))
    except PermissionError:
        # Если нет доступа к папке, пропустить её
        file.write(prefix + "└── [Доступ запрещён]\n")
        return

    # Фильтруем скрытые файлы и папки
    items = [item for item in items if not is_hidden(item)]

    for index, item in enumerate(items):
        path = os.path.join(dir_path, item)
        connector = "├── " if index < len(items) - 1 else "└── "
        file.write(prefix + connector + item + "\n")
        if os.path.isdir(path):
            extension = "│   " if index < len(items) - 1 else "    "
            generate_tree(path, file, prefix + extension)

if __name__ == "__main__":
    # Укажите путь к вашей папке
    target_dir = "/Users/andreigrishin/Documents/GitHub/NovoInvest"
    output_file = os.path.join(target_dir, "tree_structure.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(target_dir + "\n")
        generate_tree(target_dir, f)

    print(f"Структура файлов сохранена в {output_file}")
