# services/db/instructions_consumer.py
import logging
import json
import asyncio
from kafka import KafkaConsumer, KafkaProducer
from .gaps_manager import GapFiller  # см. далее
from .config import DB_KAFKA_INSTRUCTIONS_TOPIC, TG_INSTRUCTIONS_TOPIC
# или где у вас хранится инфа о топиках
# Предположим, есть db.config.py, где DB_INSTRUCTIONS_TOPIC="db_instructions", TG_INSTRUCTIONS_TOPIC="tg_instructions"

logger = logging.getLogger("db_instructions_consumer")

class DBInstructionsConsumer:
    def __init__(self, db_dsn, kafka_bootstrap_servers):
        self.db_dsn = db_dsn
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.consumer = None
        self.producer = None
        self.gap_filler = GapFiller(db_dsn)  # класс, который ищет пропуски внутри БД

    def start(self):
        self.consumer = KafkaConsumer(
            DB_KAFKA_INSTRUCTIONS_TOPIC,
            bootstrap_servers=self.kafka_bootstrap_servers,
            # остальные параметры...
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            # ...
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info("DBInstructionsConsumer запущен, подписка на %s", DB_KAFKA_INSTRUCTIONS_TOPIC)

        # Основной цикл
        for message in self.consumer:
            self.handle_message(message)

    def handle_message(self, message):
        """Разбираем JSON и действуем по action."""
        try:
            data = json.loads(message.value)
            action = data.get("action")
            if action == "FIND_GAPS":
                chat_id = data["chat_id"]
                logger.info(f"Получена команда FIND_GAPS для chat_id={chat_id}")
                # ищем пропуски
                missing_ranges = self.gap_filler.find_gaps_in_db(chat_id)
                # missing_ranges — список [(start, end), (start2, end2), ...]
                for (gap_start, gap_end) in missing_ranges:
                    offset_id = gap_end + 1
                    # Отправляем команду в tg_instructions
                    cmd = {
                        "action": "SET_BACKFILL",
                        "chat_id": chat_id,
                        "offset_id": offset_id
                    }
                    self.producer.send(TG_INSTRUCTIONS_TOPIC, cmd)
                    logger.info(f"[FIND_GAPS] Отправлен SET_BACKFILL chat_id={chat_id} offset_id={offset_id}")
                # Если нужно, ещё проверяем earliest_in_db < earliest_in_telegram...
                # (Но earliest_in_telegram без запроса к tg_ubot мы не узнаем.)
            else:
                logger.warning(f"Неизвестная action={action} в db_instructions")
        except Exception as e:
            logger.exception(f"Ошибка при обработке инструкции: {e}")
