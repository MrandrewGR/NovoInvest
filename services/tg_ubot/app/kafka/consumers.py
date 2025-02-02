# services/tg_ubot/app/kafka/consumers.py

import json
import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from kafka import KafkaConsumer
from app.config import settings

logger = logging.getLogger("kafka_consumers")


class AIOKafkaMessageConsumer:
    """
    Асинхронный Consumer (aiokafka) — например, для gap_scan_response.
    """

    def __init__(self, topics, group_id):
        self.topics = topics
        self.group_id = group_id
        self.consumer = None

    async def initialize(self):
        """
        Создаём AIOKafkaConsumer, подписываемся на список топиков.
        """
        try:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.KAFKA_BROKER,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode('utf-8')
            )
            await self.consumer.start()
            logger.info(f"AIOKafkaConsumer initialized, listening on {self.topics}")
        except Exception as e:
            logger.error(f"Error initializing AIOKafkaConsumer: {e}")
            raise

    async def listen(self):
        """
        Асинхронный генератор: yield (topic, data) при каждом сообщении.
        """
        try:
            async for msg in self.consumer:
                try:
                    data = json.loads(msg.value)
                    yield msg.topic, data
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in message: {msg.value}")
        except KafkaError as e:
            logger.error(f"AIOKafkaConsumer error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unknown error in AIOKafkaConsumer: {e}")
            raise

    async def close(self):
        if self.consumer:
            await self.consumer.stop()
            logger.info("AIOKafkaConsumer closed.")


class TGInstructionsConsumer:
    """
    Отдельный Consumer, читающий синхронно через kafka-python (KafkaConsumer),
    слушает топик 'tg_instructions'. Запускать в фоновом потоке (run_in_executor).
    """

    def __init__(self, state_mgr):
        self.state_mgr = state_mgr
        self.consumer = None

    async def initialize(self):
        loop = asyncio.get_running_loop()

        def create_consumer():
            return KafkaConsumer(
                "tg_instructions",
                bootstrap_servers=[settings.KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='tg_instructions_group'
            )
        self.consumer = await loop.run_in_executor(None, create_consumer)
        logger.info("TGInstructionsConsumer initialized (listening 'tg_instructions').")

    async def listen(self):
        """
        В бесконечном цикле читаем .consumer (блокирующий),
        обрабатываем инструкцию.
        """
        while True:
            try:
                def get_next():
                    return next(iter(self.consumer))

                message = await asyncio.get_running_loop().run_in_executor(None, get_next)
                self.handle_instruction(message)
            except StopIteration:
                logger.debug("No instructions in tg_instructions right now.")
                await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Error reading tg_instructions: {e}")
                await asyncio.sleep(5)

    def handle_instruction(self, message):
        """
        Разбираем JSON, экшен 'SET_BACKFILL'.
        """
        try:
            data = json.loads(message.value)
            action = data.get("action")
            if action == "SET_BACKFILL":
                chat_id = data["chat_id"]
                offset_id = data["offset_id"]
                logger.info(f"[TGInstructionsConsumer] set backfill_from_id={offset_id} for chat={chat_id}")
                self.state_mgr.update_backfill_from_id(chat_id, offset_id)
            else:
                logger.warning(f"[TGInstructionsConsumer] Unknown action: {action}")
        except Exception as e:
            logger.exception(f"[TGInstructionsConsumer] Error handle_instruction: {e}")
