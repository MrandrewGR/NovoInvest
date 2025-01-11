# File location: services/db/process_messages.py

import os
import json
import logging
import re
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError
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


def sanitize_table_name(name_uname):
    """
    Преобразует name_uname в допустимое имя таблицы.
    Заменяет недопустимые символы на '_'.
    Если name_uname не строка, преобразует в строку.
    """
    if not isinstance(name_uname, str):
        name_uname = str(name_uname)
    # Удаляем ведущий '@', если присутствует
    name_uname = name_uname.lstrip('@')
    # Заменяем все не-алфавитно-цифровые символы на '_'
    name_uname = re.sub(r'\W+', '_', name_uname)
    # Убеждаемся, что имя начинается с буквы или '_'
    if not re.match(r'^[A-Za-z_]', name_uname):
        name_uname = f"_{name_uname}"
    # Ограничиваем длину имени до 63 символов (ограничение PostgreSQL)
    name_uname = name_uname[:63]
    return name_uname.lower()


def get_table_name(name_uname, target_id):
    """
    Формирует допустимое имя таблицы на основе name_uname или target_id.
    Если name_uname недоступно или равно "Unknown", используется target_id.
    """
    if name_uname and name_uname != "Unknown":
        sanitized = sanitize_table_name(name_uname)
    else:
        # Для отрицательных target_id (каналы): messages_neg100123456
        # Для положительных target_id (чаты): messages_123456
        if target_id < 0:
            sanitized = f"messages_neg{abs(target_id)}"
        else:
            sanitized = f"messages_{target_id}"
    return sanitized


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


def ensure_table_exists(conn, table_name):
    """
    Создаёт партиционированную таблицу под данный table_name, если она не существует.
    PRIMARY KEY включает month_part.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (table_name,))
        exists = cur.fetchone()[0]
        if not exists:
            logger.info(f"Создание таблицы {table_name}")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    id SERIAL,
                    data JSONB NOT NULL,
                    month_part DATE NOT NULL,
                    PRIMARY KEY (id, month_part)
                )
                PARTITION BY RANGE (month_part);
            """)
            conn.commit()
            logger.info(f"Таблица {table_name} успешно создана.")


def ensure_partition_exists(conn, table_name, month_part):
    """
    Создаёт партицию для таблицы (на основе month_part), если она не существует.
    month_part в формате "YYYY-MM".
    """
    start_date = datetime.strptime(month_part, '%Y-%m').date()  # 1-е число месяца
    partition_name = f"{table_name}_{month_part.replace('-', '_')}"

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (partition_name,))
        exists = cur.fetchone()[0]

        if not exists:
            logger.info(f"Создание партиции {partition_name}")
            # Определяем границы диапазона
            if start_date.month == 12:
                end_date = datetime(start_date.year + 1, 1, 1).date()
            else:
                end_date = datetime(start_date.year, start_date.month + 1, 1).date()

            cur.execute(f"""
                CREATE TABLE {partition_name}
                PARTITION OF {table_name}
                FOR VALUES FROM (%s) TO (%s);
            """, (start_date, end_date))
            conn.commit()
            logger.info(f"Партиция {partition_name} успешно создана.")


def insert_message(conn, table_name, month_part, message_data):
    """
    Вставляет сообщение в соответствующую таблицу и партицию.
    Если в message_data нет "month_part", используем текущий месяц.
    """
    month_part_str = message_data.get("month_part")
    if not month_part_str:
        month_part_str = datetime.now().strftime('%Y-%m')
    month_part_date = datetime.strptime(month_part_str, '%Y-%m').date()

    with conn.cursor() as cur:
        try:
            cur.execute(f"""
                INSERT INTO {table_name} (data, month_part)
                VALUES (%s, %s)
            """, (json.dumps(message_data), month_part_date))
            conn.commit()
            logger.info(f"Сообщение вставлено в {table_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при вставке сообщения в {table_name}: {e}")
            raise


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
    name_uname = data.get("name_uname")

    if (name_uname is None and target_id is None) or month_part is None:
        logger.warning(f"Пропускаем сообщение без target_id или month_part: {data}")
        return

    table_name = get_table_name(name_uname, target_id)

    ensure_table_exists(conn, table_name)
    ensure_partition_exists(conn, table_name, month_part)
    insert_message(conn, table_name, month_part, data)


def run_consumer():
    """
    Потребитель Kafka, читающий сообщения и раскладывающий их в разные партиционированные таблицы.
    При ошибке соединения к БД делаем reconnect и ПОВТОРЯЕМ вставку того же сообщения.
    """
    conn = create_connection()

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
                    process_single_message(conn, raw_json)
                    break  # успешная вставка — выходим из цикла while
                except json.JSONDecodeError:
                    logger.error("Не удалось декодировать JSON в сообщении.")
                    break  # Нет смысла повторять, JSON «битый»
                except (OperationalError, InterfaceError) as db_err:
                    logger.error(f"Проблемы с соединением к БД: {db_err}", exc_info=True)
                    # Закроем коннект и пересоздадим
                    try:
                        conn.close()
                    except:
                        pass

                    # Увеличим счётчик попыток
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.info(f"Пробуем переподключиться и повторить (попытка #{retry_count})...")
                        conn = create_connection()
                        continue  # повторить ту же вставку
                    else:
                        logger.error("Превышено число повторных попыток для одного сообщения. Пропускаем.")
                        break
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
                    # Это не ошибка соединения, значит, повторять бессмысленно (или логика индивидуальная)
                    break

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    except Exception as e:
        logger.error(f"Неизвестная ошибка в цикле consumer: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info("Соединение с БД закрыто.")


if __name__ == "__main__":
    run_consumer()
