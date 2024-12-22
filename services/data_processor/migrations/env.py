from logging.config import fileConfig
import os
import sys
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# Добавьте путь к PYTHONPATH при необходимости
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Импорт вашей базы и моделей
from app.database import Base

# Это объект метаданных, который нужен Alembic для автоматической генерации миграций
target_metadata = Base.metadata

# Этот объект позволяет читать настройки из alembic.ini
config = context.config
fileConfig(config.config_file_name)


def run_migrations_offline():
    """Run migrations in 'offline' mode."""
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
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
