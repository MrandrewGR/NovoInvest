# File location: ./services/nsd_worker/app/config.py

import os
from mirco_services_data_management.config import BaseConfig

class NsdWorkerConfig(BaseConfig):
    """
    Configuration class for the nsd_worker service.
    Inherits from BaseConfig for Kafka/DB settings,
    plus adds NSD_PARENT_TABLE and TG_UBOT_NSD_TABLE.
    """

    # Kafka
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
    KAFKA_CONSUME_TOPIC = os.getenv('KAFKA_CONSUME_TOPIC', 'tg_ubot_output')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'nsd_worker_group')
    KAFKA_PRODUCE_TOPIC = os.getenv('KAFKA_PRODUCE_TOPIC', 'nsd_processed_output')

    # Postgres (generic)
    DB_HOST = os.getenv('DB_HOST', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_NAME = os.getenv('DB_NAME', 'my_database')

    # DB names used specifically by nsd_worker
    NSD_WORKER_DB_NAME = os.getenv('NSD_WORKER_DB_NAME', 'nsd_worker_db')
    TG_UBOT_DB_NAME = os.getenv('TG_UBOT_DB_NAME', 'tg_ubot')

    # -------------------------------------------------------------------------
    # IMPORTANT: Add these two so nsd_worker.py won't fail:
    # -------------------------------------------------------------------------
    NSD_PARENT_TABLE = os.getenv('NSD_PARENT_TABLE', 'nsd_parent_table')
    TG_UBOT_NSD_TABLE = os.getenv('TG_UBOT_NSD_TABLE', 'messages_nsdfeed_ru')
    # -------------------------------------------------------------------------

    # Poll interval for DB fallback
    POLL_INTERVAL_DB = int(os.getenv('POLL_INTERVAL_DB', '60'))

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # For random delay
    DELAY_MIN = int(os.getenv('DELAY_MIN', '3'))
    DELAY_MAX = int(os.getenv('DELAY_MAX', '15'))

    # Optional: Kafka transactions if needed (otherwise ignore)
    TRANSACTIONAL = os.getenv('TRANSACTIONAL', 'False').lower() in ('true', '1', 'yes')
    TRANSACTIONAL_ID = os.getenv('TRANSACTIONAL_ID', 'my_transaction_id')
