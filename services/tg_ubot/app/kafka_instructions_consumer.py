# services/tg_ubot/app/kafka_instructions_consumer.py

import json
import asyncio
import logging
from kafka import KafkaConsumer
from .config import settings

logger = logging.getLogger("kafka_instructions_consumer")

class TGInstructionsConsumer:
    """
    A separate consumer that uses the synchronous `kafka` library
    in a background thread to handle messages from the 'tg_instructions' topic.
    """

    def __init__(self, state_mgr):
        self.state_mgr = state_mgr
        self.consumer = None

    async def initialize(self):
        loop = asyncio.get_running_loop()
        # Run the consumer in a thread pool
        self.consumer = await loop.run_in_executor(None, lambda: KafkaConsumer(
            "tg_instructions",
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='tg_instructions_group'
        ))
        logger.info("TGInstructionsConsumer initialized (listening on 'tg_instructions').")

    async def listen(self):
        while True:
            try:
                # Blocking call in a separate thread via run_in_executor
                message = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: next(iter(self.consumer))
                )
                self.handle_instruction(message)
            except StopIteration:
                logger.debug("No instructions in tg_instructions at the moment.")
                await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Error reading tg_instructions: {e}")
                await asyncio.sleep(5)

    def handle_instruction(self, message):
        try:
            data = json.loads(message.value)
            action = data.get("action")
            if action == "SET_BACKFILL":
                chat_id = data["chat_id"]
                offset_id = data["offset_id"]
                logger.info(f"[TGInstructionsConsumer] Setting backfill_from_id={offset_id} for chat {chat_id}")
                self.state_mgr.update_backfill_from_id(chat_id, offset_id)
            else:
                logger.warning(f"[TGInstructionsConsumer] Unknown instruction action: {action}")
        except Exception as e:
            logger.exception(f"[TGInstructionsConsumer] Error handling instruction: {e}")
