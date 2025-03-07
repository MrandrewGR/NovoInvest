# services/db/process_messages.py

import os
import json
import logging
import re
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("process_messages")

# Читаем переменные окружения
DB_HOST = os.environ.get("DB_HOST", "ni-postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
DB_NAME = os.environ.get("DB_NAME", "tg_ubot")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "ni-kafka:9092")

# Топики
KAFKA_UBOT_OUTPUT_TOPIC = os.environ.get("KAFKA_UBOT_OUTPUT_TOPIC", "ni-ubot-out")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "db_consumer_group")

# Новые переменные — топики для gap-scan
KAFKA_GAP_SCAN_TOPIC = os.environ.get("KAFKA_GAP_SCAN_TOPIC", "ni-gap-scan-request")
KAFKA_GAP_SCAN_RESPONSE_TOPIC = os.environ.get("KAFKA_GAP_SCAN_RESPONSE_TOPIC", "ni-gap-scan-response")


def sanitize_table_name(name_uname):
    if not isinstance(name_uname, str):
        name_uname = str(name_uname)
    name_uname = name_uname.lstrip('@')
    name_uname = re.sub(r'\W+', '_', name_uname)
    if not re.match(r'^[A-Za-z_]', name_uname):
        name_uname = f"_{name_uname}"
    name_uname = name_uname[:63]
    return name_uname.lower()


def get_table_name(name_uname, target_id):
    if name_uname and name_uname != "Unknown":
        sanitized = f"messages_{sanitize_table_name(name_uname)}"
    else:
        if target_id < 0:
            sanitized = f"messages_neg{abs(target_id)}"
        else:
            sanitized = f"messages_{target_id}"
    return sanitized.lower()


def create_connection():
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
        else:
            logger.debug(f"Таблица {table_name} уже существует.")


def ensure_partition_exists(conn, table_name, month_part):
    start_date = datetime.strptime(month_part, '%Y-%m').date()
    partition_name = f"{table_name}_{month_part.replace('-', '_')}"

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (partition_name,))
        exists = cur.fetchone()[0]
        if not exists:
            logger.info(f"Создание партиции {partition_name}")
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
    month_part_str = month_part
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
    data = json.loads(raw_json)
    target_id = data.get("target_id")
    month_part = data.get("month_part")
    name_uname = data.get("name_uname", "Unknown")

    if (name_uname is None and target_id is None) or month_part is None:
        logger.warning(f"Пропускаем сообщение без target_id или month_part: {data}")
        return

    table_name = get_table_name(name_uname, target_id)
    ensure_table_exists(conn, table_name)
    ensure_partition_exists(conn, table_name, month_part)
    insert_message(conn, table_name, month_part, data)


def get_earliest_in_db(conn, chat_id: int, table_name: str):
    try:
        with conn.cursor() as cur:
            sql = f"SELECT MIN((data->>'message_id')::bigint) FROM {table_name}"
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        conn.rollback()
        logger.warning(f"Ошибка при get_earliest_in_db(chat_id={chat_id}): {e}")
        return None


def get_partitions_for_chat(conn, chat_id: int, table_name: str):
    try:
        with conn.cursor() as cur:
            sql = f"""
                SELECT inhrelid::regclass::text
                FROM pg_inherits
                WHERE inhparent = '{table_name}'::regclass
            """
            cur.execute(sql)
            rows = cur.fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        conn.rollback()
        logger.warning(f"Ошибка при get_partitions_for_chat(chat_id={chat_id}): {e}")
        return []


def find_missing_ids_in_partition(conn, partition_name: str):
    try:
        with conn.cursor() as cur:
            sql = f"""
              SELECT (data->>'message_id')::bigint AS mid
              FROM {partition_name}
              ORDER BY mid
            """
            cur.execute(sql)
            all_ids = [row[0] for row in cur.fetchall()]
    except Exception as e:
        conn.rollback()
        logger.warning(f"Ошибка при find_missing_ids_in_partition({partition_name}): {e}")
        return []

    missing_ranges = []
    if not all_ids:
        return missing_ranges

    prev_id = all_ids[0]
    for i in range(1, len(all_ids)):
        current_id = all_ids[i]
        if current_id - prev_id > 1:
            gap_start = prev_id + 1
            gap_end = current_id - 1
            missing_ranges.append((gap_start, gap_end))
        prev_id = current_id
    return missing_ranges


def process_gap_scan_request(conn, message: dict, producer: KafkaProducer):
    chat_id = message.get("chat_id")
    correlation_id = message.get("correlation_id", "unknown")
    name_uname = message.get("name_uname", "Unknown")
    if not chat_id:
        logger.warning("gap_scan_request без chat_id, пропускаем")
        return

    table_name = get_table_name(name_uname, chat_id)

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (table_name,))
        exists = cur.fetchone()[0]
        if not exists:
            logger.warning(f"Таблица {table_name} не существует. Инициируем бэкфилл для чата {chat_id}.")
            backfill_request = {
                "type": "init_backfill",
                "chat_id": chat_id,
                "name_uname": name_uname,
                "correlation_id": correlation_id
            }
            producer.send(KAFKA_GAP_SCAN_RESPONSE_TOPIC, value=backfill_request)
            return

    earliest_db = get_earliest_in_db(conn, chat_id, table_name)
    partitions = get_partitions_for_chat(conn, chat_id, table_name)
    all_missing = []
    for p in partitions:
        miss = find_missing_ids_in_partition(conn, p)
        if miss:
            all_missing.extend(miss)

    response = {
        "type": "gap_scan_response",
        "chat_id": chat_id,
        "correlation_id": correlation_id,
        "earliest_in_db": earliest_db,
        "missing_ranges": all_missing,
    }
    producer.send(KAFKA_GAP_SCAN_RESPONSE_TOPIC, value=response)
    logger.info(f"[process_gap_scan_request] Отправили gap_scan_response для chat_id={chat_id}, correlation_id={correlation_id}")


def run_consumer():
    conn = create_connection()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    consumer = KafkaConsumer(
        KAFKA_UBOT_OUTPUT_TOPIC,
        KAFKA_GAP_SCAN_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: m.decode('utf-8')
    )
    logger.info(f"[db-process] Подписка на топики: {KAFKA_UBOT_OUTPUT_TOPIC}, {KAFKA_GAP_SCAN_TOPIC}")

    try:
        for msg in consumer:
            raw_json = msg.value
            topic = msg.topic
            try:
                data = json.loads(raw_json)
            except json.JSONDecodeError:
                logger.error(f"Некорректный JSON: {raw_json}")
                continue

            if topic == KAFKA_UBOT_OUTPUT_TOPIC:
                retry_count = 0
                max_retries = 2
                while True:
                    try:
                        process_single_message(conn, raw_json)
                        break
                    except (OperationalError, InterfaceError):
                        logger.error("Проблемы с БД (process_single_message)", exc_info=True)
                        try:
                            conn.close()
                        except:
                            pass
                        retry_count += 1
                        if retry_count <= max_retries:
                            logger.info(f"Reconnecting to DB, attempt #{retry_count}")
                            conn = create_connection()
                            continue
                        else:
                            logger.error("Превышено число повторных попыток.")
                            break
                    except Exception as e:
                        logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
                        break

            elif topic == KAFKA_GAP_SCAN_TOPIC:
                logger.info(f"[db-process] Получен gap_scan_request: {data}")
                try:
                    process_gap_scan_request(conn, data, producer)
                except Exception as e:
                    logger.error(f"Ошибка в process_gap_scan_request: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Consumer остановлен пользователем.")
    except Exception as e:
        logger.error(f"Ошибка в run_consumer: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info("DB соединение закрыто.")
        producer.close()
        logger.info("KafkaProducer закрыт.")


if __name__ == "__main__":
    run_consumer()
