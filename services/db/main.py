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
    """Подключается к базе 'postgres' и создаёт dbname, если её нет."""
    conn = psycopg2.connect(
        dbname='postgres',
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
    """Создаём любые расширения или схемы, если нужно."""
    with conn.cursor() as cur:
        # Например: cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        pass
    conn.commit()

def main():
    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    db_name = os.environ.get("DB_NAME", "tg_ubot")

    ensure_database_exists(db_name, db_user, db_password, db_host, db_port)

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
