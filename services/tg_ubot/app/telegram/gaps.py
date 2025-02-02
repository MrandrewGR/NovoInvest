# services/tg_ubot/app/telegram/gaps.py

import asyncio
import logging
import uuid

logger = logging.getLogger("gaps_manager")

class GapsManager:
    """
    Отвечает за логику gap_scan_request/response через Kafka,
    чтобы определить "дыры" и обновить state_mgr (backfill_from_id).
    """

    def __init__(self,
                 kafka_producer,
                 kafka_consumer,
                 state_mgr,
                 client,
                 chat_id_to_data,
                 gap_scan_request_topic="gap_scan_request",
                 gap_scan_response_topic="gap_scan_response"):
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.state_mgr = state_mgr
        self.client = client
        self.chat_id_to_data = chat_id_to_data
        self.gap_scan_request_topic = gap_scan_request_topic
        self.gap_scan_response_topic = gap_scan_response_topic

        # Ожидания ответов: {correlation_id: {"chat_id":..., "future": ...}}
        self.pending_tasks = {}

    async def find_and_fill_gaps_for_chat(self, chat_id: int):
        """
        1) Отправляем gap_scan_request
        2) Ждём ответа gap_scan_response
        3) Анализируем earliest_in_db, missing_ranges
        4) Обновляем state_mgr
        """
        correlation_id = str(uuid.uuid4())
        chat_info = self.chat_id_to_data.get(chat_id, {})
        name_uname = chat_info.get("name_uname", "Unknown")

        req = {
            "type": "gap_scan_request",
            "chat_id": chat_id,
            "correlation_id": correlation_id,
            "name_uname": name_uname
        }
        await self.kafka_producer.send_message(self.gap_scan_request_topic, req)
        logger.info(f"[GapsManager] Sent gap_scan_request for chat_id={chat_id}, correlation_id={correlation_id}")

        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.pending_tasks[correlation_id] = {"chat_id": chat_id, "future": fut}

        # Ждём ответ не более 30 сек
        try:
            response = await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning(f"[GapsManager] No gap_scan_response for chat_id={chat_id}")
            del self.pending_tasks[correlation_id]
            return

        # Обрабатываем ответ
        response_type = response.get("type")
        if response_type == "gap_scan_response":
            earliest_in_db = response.get("earliest_in_db")
            missing_ranges = response.get("missing_ranges", [])
            logger.info(f"[GapsManager] chat_id={chat_id}, earliest_in_db={earliest_in_db}, missing={missing_ranges}")

            earliest_in_tg = await self._get_earliest_in_telegram(chat_id)
            # Если в БД самое раннее сообщение 'earliest_in_db' гораздо позже, чем самое раннее в TG
            if earliest_in_db and earliest_in_tg and earliest_in_db > (earliest_in_tg + 1):
                self.state_mgr.update_backfill_from_id(chat_id, earliest_in_db)
                logger.info(f"[GapsManager] Set backfill_from_id={earliest_in_db}")

            # Сдвигаем backfill_from_id, чтобы пропустить "дырявые" диапазоны
            for (start, end) in missing_ranges:
                bf_from = end + 1
                self.state_mgr.update_backfill_from_id(chat_id, bf_from)
                logger.info(f"[GapsManager] Skipping {start}..{end}, set backfill_from_id={bf_from}")

        elif response_type == "init_backfill":
            logger.info(f"[GapsManager] init_backfill => update backfill_from_id=None")
            self.state_mgr.update_backfill_from_id(chat_id, None)
        else:
            logger.warning(f"[GapsManager] Unknown response type={response_type}")

    async def _get_earliest_in_telegram(self, chat_id: int):
        """
        Запрашиваем самое раннее сообщение (limit=1, offset=0, reverse=True).
        """
        try:
            msgs = await self.client.get_messages(chat_id, limit=1, offset_id=0, reverse=True)
            if msgs:
                return msgs[0].id
            return None
        except Exception as e:
            logger.warning(f"[GapsManager] _get_earliest_in_telegram({chat_id}) err: {e}")
            return None

    async def handle_gap_scan_response(self, data: dict):
        """
        Вызывается извне, когда приходит gap_scan_response из Kafka.
        Сопоставляет correlation_id и завершает future.
        """
        correlation_id = data.get("correlation_id")
        if not correlation_id:
            logger.warning("[GapsManager] gap_scan_response w/o correlation_id?")
            return

        task_info = self.pending_tasks.pop(correlation_id, None)
        if not task_info:
            logger.warning(f"[GapsManager] No pending task for correlation_id={correlation_id}")
            return

        fut = task_info["future"]
        if not fut.done():
            fut.set_result(data)
        else:
            logger.debug(f"[GapsManager] Future already done for correlation_id={correlation_id}")
