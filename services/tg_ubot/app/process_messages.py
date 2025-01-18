# services/tg_ubot/app/process_messages.py

import re

def sanitize_table_name(name_uname):
    """
    Очищает имя пользователя для использования в названии таблицы.
    Заменяет неалфавитно-цифровые символы на подчеркивания и приводит к нижнему регистру.
    """
    if not isinstance(name_uname, str):
        name_uname = str(name_uname)
    # Удаляем ведущий '@'
    name_uname = name_uname.lstrip('@')
    # Заменяем все не-буквенно-цифровые символы на '_'
    name_uname = re.sub(r'\W+', '_', name_uname)
    # Убедимся, что имя начинается с буквы или '_'
    if not re.match(r'^[A-Za-z_]', name_uname):
        name_uname = f"_{name_uname}"
    # Ограничиваем длину до 63 символов (ограничение PostgreSQL)
    name_uname = name_uname[:63]
    return name_uname.lower()

def get_table_name(name_uname, target_id):
    """
    Возвращает очищенное имя таблицы на основе name_uname или target_id.
    Формат: messages_{name_uname}
    """
    if name_uname and name_uname != "Unknown":
        sanitized = f"messages_{sanitize_table_name(name_uname)}"
    else:
        # Если name_uname отсутствует, используем target_id
        if target_id < 0:
            sanitized = f"messages_neg{abs(target_id)}"
        else:
            sanitized = f"messages_{target_id}"
    return sanitized.lower()
