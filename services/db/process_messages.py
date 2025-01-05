# services/db/process_messages.py

import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("process_messages")

# Считываем переменные окружения
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
DB_NAME = os.environ.get("DB_NAME", "tg_ubot")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "db_consumer_group")


def create_connection():
    """Подключение к PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Не удалось подключиться к базе данных: {e}")
        raise

def get_table_name(target_id):
    """
    Преобразование target_id в допустимое имя таблицы.
    Например, для отрицательного ID -100123456
    будет 'messages_neg100123456', для положительного 'messages_1234'.
    """
    if target_id < 0:
        return f"messages_neg{abs(target_id)}"
    return f"messages_{target_id}"

def ensure_table_exists(conn, target_id):
    """
    Создаём "главную" таблицу под данный target_id, если она не существует,
    с партиционированием по RANGE (created_at).
    """
    table_name = get_table_name(target_id)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (table_name,))
        exists = cur.fetchone()[0]
        if not exists:
            logger.info(f"Создание таблицы {table_name}")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
                PARTITION BY RANGE (created_at);
            """)
            conn.commit()
            logger.info(f"Таблица {table_name} успешно создана.")

def ensure_partition_exists(conn, target_id, month_part):
    """
    Создаём партицию для таблицы (на основе month_part), если она не существует.
    month_part в формате "YYYY-MM".
    """
    table_name = get_table_name(target_id)
    partition_name = f"{table_name}_{month_part.replace('-', '_')}"

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (partition_name,))
        exists = cur.fetchone()[0]

        if not exists:
            logger.info(f"Создание партиции {partition_name}")
            # Диапазон: [start_of_month, start_of_next_month)
            start_date = datetime.strptime(month_part, '%Y-%m')
            if start_date.month == 12:
                end_date = datetime(start_date.year + 1, 1, 1)
            else:
                end_date = datetime(start_date.year, start_date.month + 1, 1)

            cur.execute(f"""
                CREATE TABLE {partition_name}
                PARTITION OF {table_name}
                FOR VALUES FROM (%s) TO (%s);
            """, (start_date, end_date))
            conn.commit()
            logger.info(f"Партиция {partition_name} успешно создана.")

def insert_message(conn, target_id, month_part, message_data):
    """
    Вставляет сообщение в соответствующую таблицу и партицию.
    created_at берём из 'date' внутри message_data.
    """
    table_name = get_table_name(target_id)
    created_at_str = message_data.get("date")  # ISO-строка
    if not created_at_str:
        # fallback: ставим текущую дату
        created_at_str = datetime.now().isoformat()

    with conn.cursor() as cur:
        try:
            cur.execute(f"""
                INSERT INTO {table_name} (data, created_at)
                VALUES (%s, %s)
            """, (json.dumps(message_data), created_at_str))
            conn.commit()
            logger.info(f"Сообщение вставлено в {table_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при вставке сообщения в {table_name}: {e}")
            raise

def run_consumer():
    """
    Потребитель Kafka, читающий сообщения и сохраняющий их в разные таблицы
    (зависящие от target_id) с партиционированием по month_part.
    """
    # Подключаемся к БД
    conn = create_connection()

    # Настраиваем KafkaConsumer
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
            try:
                raw_json = message.value  # уже str после value_deserializer
                data = json.loads(raw_json)

                target_id = data.get("target_id")
                month_part = data.get("month_part")

                if target_id is None or month_part is None:
                    logger.warning(f"Пропускаем сообщение без target_id/month_part: {data}")
                    continue

                # 1. Гарантируем, что таблица под этот target_id есть
                ensure_table_exists(conn, target_id)

                # 2. Гарантируем, что партиция за этот month_part есть
                ensure_partition_exists(conn, target_id, month_part)

                # 3. Вставляем сообщение
                insert_message(conn, target_id, month_part, data)

            except json.JSONDecodeError:
                logger.error("Не удалось декодировать JSON в сообщении.")
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в цикле consumer: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info("Соединение с БД закрыто.")

if __name__ == "__main__":
    run_consumer()
