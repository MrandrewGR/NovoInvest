# services/db/process_messages.py
import os
import json
import logging
from datetime import datetime

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, InterfaceError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("process_messages")

# Читаем переменные окружения
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
DB_NAME = os.environ.get("DB_NAME", "tg_ubot")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "db_consumer_group")


def create_engine_connection():
    """
    Создаёт SQLAlchemy Engine для подключения к PostgreSQL.
    """
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url, future=True)
    return engine


def get_table_name(target_id):
    """
    Преобразование target_id в допустимое имя таблицы.
    Для отрицательного: -100123456 => messages_neg100123456
    Для положительного: 123456 => messages_123456
    """
    if target_id < 0:
        return f"messages_neg{abs(target_id)}"
    return f"messages_{target_id}"


def ensure_table_exists(conn, target_id):
    """
    Создаём "родительскую" партиционированную таблицу под данный target_id, если она не существует.
    PRIMARY KEY включает month_part.
    """
    table_name = get_table_name(target_id)

    # Проверяем, существует ли таблица
    check_sql = text("SELECT to_regclass(:table_name);")
    exists = conn.execute(check_sql, {"table_name": table_name}).scalar()

    if not exists:
        logger.info(f"Создание таблицы {table_name}")
        create_sql = text(f"""
            CREATE TABLE {table_name} (
                id SERIAL,
                data JSONB NOT NULL,
                month_part DATE NOT NULL,
                PRIMARY KEY (id, month_part)
            )
            PARTITION BY RANGE (month_part);
        """)
        conn.execute(create_sql)
        logger.info(f"Таблица {table_name} успешно создана.")


def ensure_partition_exists(conn, target_id, month_part):
    """
    Создаём партицию для таблицы (на основе month_part), если она не существует.
    month_part в формате "YYYY-MM".
    """
    table_name = get_table_name(target_id)
    partition_name = f"{table_name}_{month_part.replace('-', '_')}"

    check_sql = text("SELECT to_regclass(:partition_name);")
    exists = conn.execute(check_sql, {"partition_name": partition_name}).scalar()

    if not exists:
        logger.info(f"Создание партиции {partition_name}")
        start_date = datetime.strptime(month_part, '%Y-%m').date()

        if start_date.month == 12:
            end_date = datetime(start_date.year + 1, 1, 1).date()
        else:
            end_date = datetime(start_date.year, start_date.month + 1, 1).date()

        create_partition_sql = text(f"""
            CREATE TABLE {partition_name}
            PARTITION OF {table_name}
            FOR VALUES FROM (:start_date) TO (:end_date);
        """)
        conn.execute(create_partition_sql, {
            "start_date": start_date,
            "end_date": end_date
        })
        logger.info(f"Партиция {partition_name} успешно создана.")


def insert_message(conn, target_id, month_part, message_data):
    """
    Вставляет сообщение в соответствующую таблицу и партицию.
    Если в message_data нет "month_part", используем текущий месяц.
    """
    table_name = get_table_name(target_id)
    month_part_str = message_data.get("month_part") or datetime.now().strftime('%Y-%m')
    month_part_date = datetime.strptime(month_part_str, '%Y-%m').date()

    insert_sql = text(f"""
        INSERT INTO {table_name} (data, month_part)
        VALUES (:data, :month_part)
    """)

    conn.execute(insert_sql, {
        "data": json.dumps(message_data),
        "month_part": month_part_date
    })
    logger.info(f"Сообщение вставлено в {table_name}")


def process_single_message(conn, raw_json):
    """
    Обработка одного сообщения (string в формате JSON).
    1) Парсим JSON
    2) Проверяем/создаём таблицу
    3) Проверяем/создаём партицию
    4) Вставляем сообщение
    """
    data = json.loads(raw_json)
    target_id = data.get("target_id")
    month_part = data.get("month_part")

    if target_id is None or month_part is None:
        logger.warning(f"Пропускаем сообщение без target_id или month_part: {data}")
        return

    ensure_table_exists(conn, target_id)
    ensure_partition_exists(conn, target_id, month_part)
    insert_message(conn, target_id, month_part, data)


def run_consumer():
    """
    Потребитель Kafka, читающий сообщения и раскладывающий их в разные партиционированные таблицы.
    При ошибке соединения к БД делаем reconnect и ПОВТОРЯЕМ вставку того же сообщения.
    """
    engine = create_engine_connection()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    logger.info(f"Подписка на Kafka topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            raw_json = message.value

            # Будем пытаться до 2 раз вставить одно и то же сообщение
            retry_count = 0
            max_retries = 2

            while True:
                try:
                    # SQLAlchemy "begin()" даёт транзакцию с автокоммитом при успехе
                    with engine.begin() as conn:
                        process_single_message(conn, raw_json)
                    break  # успешная вставка — выходим из цикла while

                except json.JSONDecodeError:
                    logger.error("Не удалось декодировать JSON в сообщении.")
                    break  # Бессмысленно повторять, JSON «битый»

                except (OperationalError, InterfaceError) as db_err:
                    # Ошибка соединения к БД
                    logger.error(f"Проблемы с соединением к БД: {db_err}", exc_info=True)
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.info(f"Пробуем переподключиться (попытка #{retry_count})...")
                        engine.dispose()  # закрываем старый Engine
                        engine = create_engine_connection()
                        continue
                    else:
                        logger.error("Превышено число повторных попыток. Пропускаем сообщение.")
                        break

                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
                    # Это не ошибка соединения, значит, повторять бессмысленно
                    break

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в цикле consumer: {e}", exc_info=True)
    finally:
        consumer.close()
        engine.dispose()
        logger.info("Консьюмер и соединение с БД закрыты.")


if __name__ == "__main__":
    run_consumer()
