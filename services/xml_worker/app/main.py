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

# Инициализация логирования (выводим сразу в stdout)
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


def produce_positions(producer: KafkaProducer, user_id: str, positions_df: pd.DataFrame):
    """
    Отправляет данные о позициях (ISIN, количество бумаг, средняя цена покупки)
    в указанный топик Kafka. Добавляем user_id, чтобы бот знал, кому отвечать.
    """
    if positions_df.empty:
        logger.info("Нет позиций для отправки в Kafka.")
        # Даже если пусто, всё равно сообщим боту, что обработка завершена (без результатов)
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

    # Формируем массив результатов
    result_data = []
    for _, row in positions_df.iterrows():
        isin = row['ISIN']
        qty = row['количество бумаг']
        avg_price = row['средняя цена покупки']
        result_data.append({
            "isin": isin,
            "quantity": qty,
            "avg_price": avg_price if avg_price is not None else None
        })

    # Отправляем одним сообщением список позиций
    msg_dict = {
        "user_id": user_id,
        "result": result_data
    }
    try:
        producer.send(
            KAFKA_PRODUCE_TOPIC,
            key=str(user_id).encode('utf-8'),
            value=json.dumps(msg_dict).encode('utf-8')
        )
        producer.flush()
        logger.info(f"Отправлено в Kafka (исходящие данные): {msg_dict}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке результата в Kafka: {e}")


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

    logger.info(f"[xml_worker] Подключились к Kafka. Ожидание сообщений из топика '{KAFKA_CONSUME_TOPIC}'...")

    for message in consumer:
        # Ожидается JSON с полями { "user_id": ..., "file_path": ... }
        msg_str = message.value
        logger.info(f"[xml_worker] Получено сообщение из Kafka: {msg_str}")
        try:
            msg_json = json.loads(msg_str)
        except json.JSONDecodeError:
            logger.exception("Сообщение не является валидным JSON.")
            continue

        # Извлекаем user_id, путь к файлу и т.д.
        user_id = msg_json.get("user_id")
        xml_file_path = msg_json.get("file_path")
        if not user_id or not xml_file_path:
            logger.warning(f"Не указаны user_id или file_path в сообщении: {msg_json}")
            continue

        # Обрабатываем XML-файл
        try:
            positions_df = process_xml_file(xml_file_path)
            # Публикуем результат в другой топик
            produce_positions(producer, user_id, positions_df)
        except Exception as e:
            logger.exception(f"Ошибка при обработке файла {xml_file_path}: {e}")


if __name__ == "__main__":
    main()
