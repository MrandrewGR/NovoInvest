# services/db/main.py

import os
import logging
import psycopg2
import psycopg2.extensions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def ensure_database_exists(dbname, user, password, host, port):
    """
    Подключается к базе 'postgres' (системной) и создаёт dbname, если её нет.
    """
    conn = psycopg2.connect(
        dbname='postgres',  # Подключаемся к «системной» БД
        user=user,
        password=password,
        host=host,
        port=port
    )
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
            exists = cur.fetchone()
            if not exists:
                logging.info(f"Database {dbname} not found. Creating...")
                cur.execute(f"CREATE DATABASE {dbname};")
                logging.info(f"Database {dbname} created successfully!")
    finally:
        conn.close()

def create_common_objects(conn):
    """
    Здесь можно создать общие объекты, например, расширения,
    схемы или базовые таблицы. При необходимости.
    """
    with conn.cursor() as cur:
        # Пример: если нужно, создаём какую-то схему:
        # cur.execute("CREATE SCHEMA IF NOT EXISTS my_schema;")
        # conn.commit()
        pass

def main():
    """
    Подготовка БД: создаём (если нет) и инициализируем общие объекты.
    """
    # Считываем переменные окружения
    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    db_name = os.environ.get("DB_NAME", "tg_ubot")

    # 1: Гарантированно убеждаемся, что нужная база данных существует.
    ensure_database_exists(db_name, db_user, db_password, db_host, db_port)

    # 2: Подключаемся к созданной БД и создаём общие объекты, если надо
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    try:
        create_common_objects(conn)
        logging.info("Common objects ensured.")
    finally:
        conn.close()
        logging.info("Connection closed.")

if __name__ == "__main__":
    main()
