# services/db/main.py

import os
import json
import time
import psycopg2.extras
import logging
import psycopg2
import psycopg2.extensions

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def create_table_if_not_exists(conn):
    """Создаёт таблицу messages и GIN-индекс по JSONB."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_data_gin
        ON messages
        USING GIN (data);
        """)
        conn.commit()


def ensure_database_exists(dbname, user, password, host, port):
    """Подключается к базе 'postgres' и создаёт dbname, если её нет."""
    # 1. Подключаемся к системной базе 'postgres'
    conn = psycopg2.connect(
        dbname='postgres',
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        # 2. Включаем режим autocommit, чтобы CREATE DATABASE не шёл в транзакции
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        # 3. Проверяем, существует ли нужная база
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
            exists = cur.fetchone()
            if not exists:
                logging.info(f"Database {dbname} not found. Creating...")
                cur.execute(f"CREATE DATABASE {dbname};")
                logging.info(f"Database {dbname} created successfully!")
    finally:
        # 4. Закрываем соединение
        conn.close()


def main():
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.environ.get("KAFKA_UBOT_OUTPUT_TOPIC", "tg_ubot_output")

    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    db_name = os.environ.get("DB_NAME", "tg_ubot")

    # 1) Гарантированно убеждаемся, что tg_ubot есть:
    ensure_database_exists(db_name, db_user, db_password, db_host, db_port)

    # Подключаемся к PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name
    )
    create_table_if_not_exists(conn)
    logging.info("Connected to PostgreSQL, ensured table structure.")

    # Настраиваем KafkaConsumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_bootstrap_servers],
        auto_offset_reset='earliest',
        enable_auto_commit=True,  # можно выключить, если нужно вручную управлять offset
        group_id='db_consumer_group'
    )
    logging.info(f"Subscribed to Kafka topic: {kafka_topic}")

    while True:
        for message in consumer:
            try:
                raw_value = message.value.decode('utf-8')
                data = json.loads(raw_value)

                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO messages (data) VALUES (%s) RETURNING id;",
                        [psycopg2.extras.Json(data)]
                    )
                    inserted_id = cur.fetchone()[0]
                    conn.commit()

                logging.info(
                    f"Inserted message into DB: offset={message.offset}, db_id={inserted_id}"
                )

            except Exception as e:
                logging.error(f"Error processing message: {e}", exc_info=True)
                conn.rollback()

        time.sleep(1)  # чтобы не крутить цикл на 100% CPU

if __name__ == "__main__":
    main()
