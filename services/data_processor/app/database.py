# File: services/data_processor/app/database.py
import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.url import make_url

# URL БД берем из окружения или дефолт
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/data_processor_db"
)

def ensure_database_exists(db_url: str):
    """
    Проверяет, существует ли база данных, и при необходимости создаёт её.
    """
    parsed_url = make_url(db_url)
    dbname = parsed_url.database

    # Подключаемся к системной базе (postgres)
    # чтобы проверить/создать нашу базу
    admin_url = parsed_url.set(database="postgres")

    import psycopg2
    with psycopg2.connect(
        dbname=admin_url.database,
        user=admin_url.username,
        password=admin_url.password,
        host=admin_url.host,
        port=admin_url.port
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as curs:
            curs.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            exists = curs.fetchone()
            if not exists:
                curs.execute(f'CREATE DATABASE "{dbname}"')
                print(f"[INFO] Created database '{dbname}'")

# Убедимся, что база есть (вызывается ещё раз из env.py Alembic, но не страшно)
ensure_database_exists(DATABASE_URL)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()
