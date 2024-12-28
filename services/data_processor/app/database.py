# services/data_processor/app/database.py

import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine.url import make_url

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/data_processor_db")

def ensure_database_exists(db_url: str):
    """
    Проверяет, существует ли база данных, и при необходимости создаёт её.
    """
    parsed_url = make_url(db_url)

    # Имя нужной базы данных
    database_name = parsed_url.database

    # Меняем в URL базу на 'postgres' (или template1),
    # чтобы подключиться к уже существующей системной базе.
    admin_url = parsed_url.set(database="postgres")

    # Подключаемся к 'postgres', чтобы проверить/создать нужную базу
    with psycopg2.connect(
        dbname=admin_url.database,
        user=admin_url.username,
        password=admin_url.password,
        host=admin_url.host,
        port=admin_url.port
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            # Проверяем, есть ли такая база
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            exists = cursor.fetchone()
            if not exists:
                # Если базы нет — создаём
                cursor.execute(f'CREATE DATABASE "{database_name}"')
                print(f"[INFO] Created database '{database_name}'")

# 1. Проверяем/создаём базу
ensure_database_exists(DATABASE_URL)

# 2. Теперь создаём движок на ту базу, которую гарантированно создали
engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()
