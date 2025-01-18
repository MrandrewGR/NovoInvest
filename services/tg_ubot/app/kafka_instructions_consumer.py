# services/tg_ubot/app/kafka_instructions_consumer.py
import json
import asyncio
import logging
from kafka import KafkaConsumer
from .config import settings

logger = logging.getLogger("kafka_instructions_consumer")

class TGInstructionsConsumer:
    def __init__(self, state_mgr):
        self.state_mgr = state_mgr
        self.consumer = None

    async def initialize(self):
        loop = asyncio.get_running_loop()
        # Запускаем consumer в отдельном потоке (run_in_executor)
        self.consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
            "tg_instructions",  # название топика
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='tg_instructions_group'
        ))
        logger.info("TGInstructionsConsumer инициализирован, слушаем tg_instructions.")

    async def listen(self):
        while True:
            try:
                # Читаем из consumer (синхронно)
                message = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: next(iter(self.consumer))
                )
                self.handle_instruction(message)
            except StopIteration:
                logger.debug("Нет команд в tg_instructions сейчас.")
                await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Ошибка при чтении tg_instructions: {e}")
                await asyncio.sleep(5)

    def handle_instruction(self, message):
        try:
            data = json.loads(message.value)
            action = data.get("action")
            if action == "SET_BACKFILL":
                chat_id = data["chat_id"]
                offset_id = data["offset_id"]
                logger.info(f"[TGInstructionsConsumer] Устанавливаем backfill_from_id={offset_id} для чата {chat_id}")
                self.state_mgr.update_backfill_from_id(chat_id, offset_id)
            else:
                logger.warning(f"[TGInstructionsConsumer] Неизвестная команда: {action}")
        except Exception as e:
            logger.exception(f"[TGInstructionsConsumer] Ошибка при handle_instruction: {e}")
