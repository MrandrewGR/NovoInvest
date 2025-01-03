# File location: ./services/xml_worker/app/broker_report_processor.py

import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

class BrokerReportParser:
    """
    Парсит XML-файл, возвращает нужные данные о позициях, у которых количество > 0
    (из раздела 'Positions' -> 'active_type_Collection' -> 'Details_Collection' -> 'Details').
    """
    def __init__(self, xml_file: str):
        self.xml_file = xml_file
        self.root = self._parse_xml()
        self._strip_namespaces(self.root)  # <-- Новый вызов для удаления namespace
        self.date_start = None
        self.date_end = None
        self._extract_report_dates()

    def _parse_xml(self):
        try:
            tree = ET.parse(self.xml_file)
            return tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Ошибка парсинга XML файла: {e}")
            raise

    def _strip_namespaces(self, elem):
        """
        Рекурсивно удаляет все пространства имён из tag,
        чтобы .findall() работал по простым именам (Positions, Report и т.д.).
        """
        for el in elem.iter():
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]

    def _extract_report_dates(self):
        """
        Если вдруг в XML есть <date_start> и <date_end>, пытаемся считать.
        Если их нет (или формат не совпадает) – оставляем None.
        """
        self.date_start = self.root.findtext('.//date_start')
        self.date_end = self.root.findtext('.//date_end')
        if self.date_start:
            for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    self.date_start = datetime.strptime(self.date_start, fmt)
                    break
                except ValueError:
                    self.date_start = None
        if self.date_end:
            for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    self.date_end = datetime.strptime(self.date_end, fmt)
                    break
                except ValueError:
                    self.date_end = None

    def parse_positions_in_period(self) -> list:
        """
        Ищет в XML блок 'Positions' -> 'active_type_Collection' -> 'Details_Collection' -> 'Details'
        и возвращает список словарей вида:
          [
            {"isin": ..., "name": ..., "quantity": ...},
            ...
          ]
        для тех, у кого quantity (берём из `real_rest`) > 0.
        """
        positions = []
        details_xpath = './/Positions//Report[@Name="1_Positions"]//Details_Collection//Details'
        details_nodes = self.root.findall(details_xpath)

        if not details_nodes:
            logger.warning("Не найдены узлы <Details> с позициями в разделе 'Positions'.")
            return positions

        for det in details_nodes:
            try:
                real_rest_str = det.get('real_rest', '0')
                real_rest = float(real_rest_str.replace(',', '.'))  # На случай, если в XML может быть запятая
                if real_rest > 0:
                    isin = det.get('ISIN1', '').strip()
                    name = det.get('active_name', '').strip()
                    positions.append({
                        "isin": isin,
                        "name": name,
                        "quantity": real_rest
                    })
            except Exception as e:
                logger.exception(f"Ошибка при парсинге элемента <Details>: {e}")

        return positions
