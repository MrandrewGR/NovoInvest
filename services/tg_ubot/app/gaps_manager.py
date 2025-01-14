# File: services/tg_ubot/app/gaps_manager.py

import asyncio
import logging
import json
import uuid
from .state_manager import StateManager

logger = logging.getLogger("gaps_manager")

class GapsManager:
    """
    Вместо прямого подключения к БД, шлём запросы в Kafka (gap_scan_request),
    получаем ответы из gap_scan_response, после чего обновляем state_mgr.
    """

    def __init__(self,
                 kafka_producer,        # Экземпляр KafkaMessageProducer
                 kafka_consumer,        # Экземпляр KafkaMessageConsumer (подписанный на gap_scan_response)
                 state_mgr: StateManager,
                 client,                # TelethonClient - чтобы получить earliest_in_telegram
                 gap_scan_request_topic="gap_scan_request",
                 gap_scan_response_topic="gap_scan_response"):
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.state_mgr = state_mgr
        self.client = client
        self.gap_scan_request_topic = gap_scan_request_topic
        self.gap_scan_response_topic = gap_scan_response_topic

        # словарь "ждущих" ответов: {correlation_id: {"chat_id":..., "future": future}}
        self.pending_tasks = {}

    async def find_and_fill_gaps_for_chat(self, chat_id: int):
        """
        1) шлём gap_scan_request
        2) ждём ответ
        3) сравниваем earliest_in_telegram
        4) выставляем backfill_from_id
        """
        correlation_id = str(uuid.uuid4())
        req = {
            "type": "gap_scan_request",
            "chat_id": chat_id,
            "correlation_id": correlation_id
        }
        # Отправляем запрос:
        await self.kafka_producer.send_message(self.gap_scan_request_topic, req)
        logger.info(f"[GapsManager] Отправлен gap_scan_request для chat_id={chat_id}, correlation_id={correlation_id}")

        # Создаём future, чтобы дождаться ответа
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.pending_tasks[correlation_id] = {"chat_id": chat_id, "future": fut}

        # Ждём ответ или таймаут, например 30с
        try:
            response = await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning(f"[GapsManager] Не дождались gap_scan_response для chat_id={chat_id}")
            return

        earliest_in_db = response.get("earliest_in_db")
        missing_ranges = response.get("missing_ranges", [])

        logger.info(f"[GapsManager] chat_id={chat_id} earliest_in_db={earliest_in_db}, missing={missing_ranges}")

        # Получаем earliest_in_telegram
        earliest_in_tg = await self._get_earliest_in_telegram(chat_id)

        if earliest_in_db and earliest_in_tg and earliest_in_db > (earliest_in_tg + 1):
            self.state_mgr.update_backfill_from_id(chat_id, earliest_in_db)
            logger.info(f"[GapsManager] Установлен backfill_from_id={earliest_in_db} (пропуск от {earliest_in_tg}..{earliest_in_db-1})")

        # Обработаем missing_ranges
        for (start, end) in missing_ranges:
            bf_from = end + 1
            self.state_mgr.update_backfill_from_id(chat_id, bf_from)
            logger.info(f"[GapsManager] Пропуск {start}..{end}, ставим backfill_from_id={bf_from} для chat={chat_id}")

    async def _get_earliest_in_telegram(self, chat_id: int):
        """
        Получаем самое раннее сообщение (Telethon): limit=1, reverse=True, offset_id=0
        """
        try:
            msgs = await self.client.get_messages(chat_id, limit=1, offset_id=0, reverse=True)
            if msgs:
                return msgs[0].id
            return None
        except Exception as e:
            logger.warning(f"[GapsManager] _get_earliest_in_telegram({chat_id}) ошибка: {e}")
            return None

    def handle_gap_scan_response(self, data: dict):
        """
        Вызывается из kafka_consumer, когда пришло сообщение type=gap_scan_response
        Ищем correlation_id, резолвим future.
        """
        correlation_id = data.get("correlation_id")
        if not correlation_id:
            logger.warning("[GapsManager] gap_scan_response без correlation_id?")
            return

        task_info = self.pending_tasks.pop(correlation_id, None)
        if not task_info:
            logger.warning(f"[GapsManager] Не нашли pending_task для correlation_id={correlation_id}")
            return

        fut = task_info["future"]
        if not fut.done():
            fut.set_result(data)
        else:
            logger.debug(f"[GapsManager] Future уже done correlation_id={correlation_id}")
