# services/db/migrations/env.py
import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context
from app.models import Base  # <-- наши модели

# Подтягиваем настройки из alembic.ini
config = context.config
fileConfig(config.config_file_name)

# Здесь Alembic поймёт структуру таблиц через метаданные моделей
target_metadata = Base.metadata

def run_migrations_offline():
    """Запуск миграций в 'оффлайн'-режиме."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Запуск миграций в 'онлайн'-режиме (с реальным подключением)."""
    section = config.get_section(config.config_ini_section)

    # Переопределим URL из переменных окружения (DB_USER, DB_PASSWORD, etc.)
    db_user = os.environ.get("DB_USER", "postgres")
    db_pass = os.environ.get("DB_PASSWORD", "postgres")
    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "tg_ubot")

    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    section["sqlalchemy.url"] = url

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
