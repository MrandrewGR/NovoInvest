# File location: ./services/xml_worker/app/broker_report_processor.py

import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)


class BrokerReportParser:
    """
    Парсит XML-файл, возвращает список сделок (trades) в виде словарей.
    """
    def __init__(self, xml_file: str):
        self.xml_file = xml_file
        self.root = self._parse_xml()
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

    def _extract_report_dates(self):
        self.date_start = self.root.findtext('.//date_start')
        self.date_end = self.root.findtext('.//date_end')
        if self.date_start:
            try:
                self.date_start = datetime.strptime(self.date_start, '%d.%m.%Y %H:%M:%S')
            except ValueError:
                self.date_start = None
        if self.date_end:
            try:
                self.date_end = datetime.strptime(self.date_end, '%d.%m.%Y %H:%M:%S')
            except ValueError:
                self.date_end = None

    def parse_trades(self) -> list:
        """
        Ищет в XML секцию со сделками, возвращает их в виде списка словарей.
        """
        trade_sections = ['.//trades_finished/trade']
        field_mapping = self._get_field_mapping()

        all_trades = []
        for section in trade_sections:
            trades = self._extract_trades(section, field_mapping)
            all_trades.extend(trades)

        return all_trades

    def _get_field_mapping(self):
        # Поля, которые нас интересуют в XML
        return {
            'db_time': 'datetime',
            'settlement_time': 'settlement_date',
            'Price': 'price',
            'qty': 'qty',
            'isin_reg': 'isin',
            'comment': 'comment'
        }

    def _extract_trades(self, section, field_mapping):
        trades = []
        for trade in self.root.findall(section):
            try:
                # Пропускаем сделки РЕПО
                if self._is_repo_trade(trade):
                    continue
                trade_data = self._parse_trade_data(trade, field_mapping)
                trades.append(trade_data)
            except Exception as e:
                logger.exception("Ошибка при парсинге сделки. Пропускаем сделку.")
        return trades

    def _is_repo_trade(self, trade) -> bool:
        comment = trade.findtext('comment', default='')
        if comment and re.search(r'РЕПО', comment, re.IGNORECASE):
            logger.info(f"Пропускаем REPO-сделку с комментарием: '{comment}'")
            return True
        return False

    def _parse_trade_data(self, trade, field_mapping):
        """
        Преобразуем XML-данные в словарь, используя field_mapping.
        """
        trade_data = {}
        for xml_field, model_field in field_mapping.items():
            value = trade.findtext(xml_field, default=None)
            trade_data[model_field] = self._convert_field(model_field, value)
        return trade_data

    def _convert_field(self, field_name, value):
        if not value:
            return None

        if field_name in ['datetime', 'settlement_date']:
            # Формат даты в XML (пример: 2023-08-01T10:15:00)
            for fmt in ('%Y-%m-%dT%H:%M:%S', '%d.%m.%Y %H:%M:%S'):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    pass
            logger.warning(f"Не удалось преобразовать дату: {value}")
            return None

        elif field_name in ['price', 'qty']:
            return float(value)

        else:
            return value


class PositionCalculator:
    """
    Класс для расчёта позиций (количество бумаг, средняя цена покупки) по датам расчётов
    на основе списка сделок (trades).
    """
    def __init__(self, trades):
        # trades — это список словарей, каждый словарь описывает сделку
        self.trades = trades

    def calculate_positions(self) -> pd.DataFrame:
        """
        Рассчитывает сводную таблицу позиций (ISIN, количество бумаг, средняя цена).
        """
        if not self.trades:
            logger.warning("Нет сделок для расчёта позиций.")
            return pd.DataFrame()

        trades_df = pd.DataFrame(self.trades)
        # Приводим settlement_date к дате (без времени), если есть
        if 'settlement_date' in trades_df.columns and trades_df['settlement_date'].notnull().any():
            trades_df['settlement_date'] = trades_df['settlement_date'].dt.date
        else:
            # Если settlement_date нет или None, ставим текущую дату,
            # чтобы хоть как-то рассчитать (или можно пропустить)
            trades_df['settlement_date'] = pd.to_datetime('today').date()

        # Сортируем по дате расчёта и дате сделки
        trades_df.sort_values(by=['settlement_date', 'datetime'], inplace=True)
        trades_df.reset_index(drop=True, inplace=True)

        # Для упрощения возьмём максимальную settlement_date (одна итоговая позиция)
        # или можно реализовать "по каждой дате" — как в предыдущем коде
        # но раз БД нет, проще сразу посчитать совокупную позицию.

        positions = {}
        for _, row in trades_df.iterrows():
            self._process_single_trade(positions, row)

        # Превращаем в DataFrame
        result = []
        for isin, pos in positions.items():
            if pos['qty'] != 0:
                result.append({
                    'ISIN': isin,
                    'количество бумаг': pos['qty'],
                    'средняя цена покупки': round(pos['avg_price'], 2) if pos['avg_price'] else None,
                })

        return pd.DataFrame(result)

    def _process_single_trade(self, positions: dict, row: pd.Series):
        isin = row.get('isin')
        price = row.get('price', 0.0)
        qty = row.get('qty', 0.0)

        if not isin:
            return

        if isin not in positions:
            positions[isin] = {'qty': 0.0, 'total_cost': 0.0, 'avg_price': None}

        pos = positions[isin]

        if qty > 0:  # покупка
            pos['qty'] += qty
            pos['total_cost'] += qty * price
        elif qty < 0:  # продажа
            # qty отрицательное, считаем по средневзвешенной (либо price)
            avg_price = pos['avg_price'] if pos['avg_price'] is not None else price
            pos['total_cost'] += qty * avg_price
            pos['qty'] += qty

        # обновляем avg_price
        if pos['qty'] != 0:
            pos['avg_price'] = pos['total_cost'] / pos['qty']
        else:
            pos['avg_price'] = None
            pos['total_cost'] = 0.0
