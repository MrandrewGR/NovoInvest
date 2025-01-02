# File location: ./services/xml_worker/app/main.py

import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUME_TOPIC,
    KAFKA_PRODUCE_TOPIC,
    LOG_LEVEL
)
from .broker_report_processor import BrokerReportParser, PositionCalculator
import pandas as pd

# Инициализация логирования
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_xml_file(xml_file_path: str) -> pd.DataFrame:
    """
    Обрабатывает указанный XML-файл, возвращает DataFrame с рассчитанными позициями.
    """
    parser = BrokerReportParser(xml_file_path)
    trades = parser.parse_trades()
    calculator = PositionCalculator(trades)
    positions_df = calculator.calculate_positions()
    return positions_df


def produce_positions(producer: KafkaProducer, positions_df: pd.DataFrame):
    """
    Отправляет данные о позициях (ISIN, количество бумаг, средняя цена покупки)
    в указанный топик Kafka.
    """
    if positions_df.empty:
        logger.info("Нет позиций для отправки в Kafka.")
        return

    for _, row in positions_df.iterrows():
        isin = row['ISIN']
        qty = row['количество бумаг']
        avg_price = row['средняя цена покупки']

        msg_dict = {
            "isin": isin,
            "quantity": qty,
            "avg_price": avg_price if avg_price is not None else None
        }
        try:
            producer.send(
                KAFKA_PRODUCE_TOPIC,
                key=isin.encode('utf-8'),
                value=json.dumps(msg_dict).encode('utf-8')
            )
            logger.info(f"Отправлено в Kafka: {msg_dict}")
        except Exception as e:
            logger.exception(f"Ошибка при отправке сообщения в Kafka: {e}")


def main():
    # Настраиваем Kafka Consumer и Producer
    consumer = KafkaConsumer(
        KAFKA_CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='xml_worker_group',
        value_deserializer=lambda x: x.decode('utf-8'),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x,  # Отправляем уже сериализованные байты
    )

    logger.info(f"Подключились к Kafka. Ожидание сообщений из топика '{KAFKA_CONSUME_TOPIC}'...")

    for message in consumer:
        xml_file_path = message.value  # путь к XML-файлу в сообщении
        logger.info(f"Получено сообщение из Kafka. Путь к XML-файлу: {xml_file_path}")

        # Обрабатываем XML-файл
        try:
            positions_df = process_xml_file(xml_file_path)
            # Публикуем результат в другой топик
            produce_positions(producer, positions_df)
        except Exception as e:
            logger.exception(f"Ошибка при обработке файла {xml_file_path}: {e}")


if __name__ == "__main__":
    main()
