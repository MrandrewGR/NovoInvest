import xml.etree.ElementTree as ET

# Загрузка XML из файла
tree = ET.parse('../tg/tgBot/inputs/xmlFiles/report1.xml')
root = tree.getroot()

positions = root.find('positions')

# Перебираем все позиции
for pos in positions.findall('position'):
    # Извлекаем количество на конец периода
    real_rest_elem = pos.find('real_rest')
    if real_rest_elem is not None:
        real_rest = float(real_rest_elem.text)
    else:
        real_rest = 0.0

    # Если количество больше 0, значит бумага есть в портфеле
    if real_rest > 0:
        active_name = pos.find('active_name').text if pos.find('active_name') is not None else ""
        ISIN = pos.find('ISIN').text if pos.find('ISIN') is not None else ""

        # Выводим необходимые данные
        print(f"Наименование: {active_name}, ISIN: {ISIN}, Кол-во: {real_rest}")
