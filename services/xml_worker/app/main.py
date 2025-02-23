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
from .broker_report_processor import BrokerReportParser
import pandas as pd

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_xml_file(xml_file_path: str):
    """
    Обрабатывает указанный XML-файл:
    1) Парсит "Динамику позиций" (находит >0 real_rest)
    2) Возвращает список словарей: [{"isin":..., "name":..., "quantity":...}, ...]
    """
    parser = BrokerReportParser(xml_file_path)
    positions = parser.parse_positions_in_period()
    return positions


def produce_positions(producer: KafkaProducer, user_id: str, positions: list):
    """
    Отправляет данные о позициях (ISIN, название, количество) в указанный топик Kafka.
    user_id - чтобы бот знал, кому отвечать.
    """
    if not positions:
        logger.info("Нет позиций для отправки в Kafka (quantity > 0 не найдены).")
        empty_msg = {
            "user_id": user_id,
            "result": []
        }
        producer.send(
            KAFKA_PRODUCE_TOPIC,
            key=str(user_id).encode("utf-8"),
            value=json.dumps(empty_msg).encode("utf-8")
        )
        producer.flush()
        return

    # Формируем единый JSON
    msg_dict = {
        "user_id": user_id,
        "result": [
            {
                "isin": pos["isin"],
                "name": pos["name"],
                "quantity": pos["quantity"]
            }
            for pos in positions
        ]
    }

    try:
        producer.send(
            KAFKA_PRODUCE_TOPIC,
            key=str(user_id).encode('utf-8'),
            value=json.dumps(msg_dict).encode('utf-8')
        )
        producer.flush()
        logger.info(f"[xml_worker] Отправлено в Kafka (исходящие данные): {msg_dict}")
    except Exception as e:
        logger.exception(f"[xml_worker] Ошибка при отправке результата в Kafka: {e}")


def main():
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
        value_serializer=lambda x: x,  # уже сериализованные байты
    )

    logger.info(f"[xml_worker] Подключились к Kafka. Ожидание сообщений из топика '{KAFKA_CONSUME_TOPIC}'...")

    for message in consumer:
        msg_str = message.value
        logger.info(f"[xml_worker] Получено сообщение из Kafka: {msg_str}")
        try:
            msg_json = json.loads(msg_str)
        except json.JSONDecodeError:
            logger.exception("[xml_worker] Сообщение не валидный JSON.")
            continue

        user_id = msg_json.get("user_id")
        xml_file_path = msg_json.get("file_path")
        if not user_id or not xml_file_path:
            logger.warning(f"[xml_worker] Не указаны user_id или file_path: {msg_json}")
            continue

        try:
            # Извлекаем нужные позиции (real_rest > 0)
            positions = process_xml_file(xml_file_path)
            # Отправляем результат в Kafka
            produce_positions(producer, user_id, positions)
        except Exception as e:
            logger.exception(f"[xml_worker] Ошибка при обработке файла {xml_file_path}: {e}")


if __name__ == "__main__":
    main()
