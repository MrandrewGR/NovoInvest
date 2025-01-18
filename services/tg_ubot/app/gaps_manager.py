# services/tg_ubot/app/gaps_manager.py

import asyncio
import logging
import json
import uuid
from .state_manager import StateManager
from .process_messages import get_table_name  # Import the function for table naming

logger = logging.getLogger("gaps_manager")


class GapsManager:
    """
    Instead of direct DB connection, sends requests to Kafka (gap_scan_request),
    receives responses from gap_scan_response, and updates state_mgr accordingly.
    """

    def __init__(self,
                 kafka_producer,        # Instance of KafkaMessageProducer
                 kafka_consumer,        # Instance of KafkaMessageConsumer (subscribed to gap_scan_response)
                 state_mgr: StateManager,
                 client,                # TelegramClient - to get earliest_in_telegram
                 chat_id_to_data,       # Added chat_id_to_data
                 gap_scan_request_topic="gap_scan_request",
                 gap_scan_response_topic="gap_scan_response"):
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.state_mgr = state_mgr
        self.client = client
        self.chat_id_to_data = chat_id_to_data  # Store chat_id_to_data
        self.gap_scan_request_topic = gap_scan_request_topic
        self.gap_scan_response_topic = gap_scan_response_topic

        # Dictionary of "pending" responses: {correlation_id: {"chat_id":..., "future": future}}
        self.pending_tasks = {}

    async def find_and_fill_gaps_for_chat(self, chat_id: int):
        """
        1) Sends a gap_scan_request
        2) Waits for the response
        3) Compares earliest_in_telegram
        4) Sets backfill_from_id
        """
        correlation_id = str(uuid.uuid4())
        name_uname = self.chat_id_to_data.get(chat_id, {}).get("name_uname", "Unknown")  # Corrected field name
        table_name = get_table_name(name_uname, chat_id)  # Use name_uname

        req = {
            "type": "gap_scan_request",
            "chat_id": chat_id,
            "correlation_id": correlation_id,
            "name_uname": name_uname  # Ensure name_uname is included in the request
        }
        # Send the request
        await self.kafka_producer.send_message(self.gap_scan_request_topic, req)
        logger.info(f"[GapsManager] Sent gap_scan_request for chat_id={chat_id}, correlation_id={correlation_id}")

        # Create a future to wait for the response
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.pending_tasks[correlation_id] = {"chat_id": chat_id, "future": fut}

        # Wait for the response or timeout (e.g., 30 seconds)
        try:
            response = await asyncio.wait_for(fut, timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning(f"[GapsManager] Did not receive gap_scan_response for chat_id={chat_id}")
            del self.pending_tasks[correlation_id]
            return

        # Process different types of responses
        response_type = response.get("type")
        if response_type == "gap_scan_response":
            earliest_in_db = response.get("earliest_in_db")
            missing_ranges = response.get("missing_ranges", [])

            logger.info(f"[GapsManager] chat_id={chat_id} earliest_in_db={earliest_in_db}, missing={missing_ranges}")

            # Get earliest_in_telegram
            earliest_in_tg = await self._get_earliest_in_telegram(chat_id)

            if earliest_in_db and earliest_in_tg and earliest_in_db > (earliest_in_tg + 1):
                self.state_mgr.update_backfill_from_id(chat_id, earliest_in_db)
                logger.info(f"[GapsManager] Set backfill_from_id={earliest_in_db} (skipping from {earliest_in_tg}..{earliest_in_db-1})")

            # Handle missing_ranges
            for (start, end) in missing_ranges:
                bf_from = end + 1
                self.state_mgr.update_backfill_from_id(chat_id, bf_from)
                logger.info(f"[GapsManager] Skipping {start}..{end}, setting backfill_from_id={bf_from} for chat={chat_id}")

        elif response_type == "init_backfill":
            # Initiate backfill for the given chat
            logger.info(f"[GapsManager] Initiating backfill for chat_id={chat_id}")
            self.state_mgr.update_backfill_from_id(chat_id, None)  # Set the necessary initial value
            # Assuming BackfillManager will periodically check and start backfill
        else:
            logger.warning(f"[GapsManager] Unknown response type: {response_type}")

    async def _get_earliest_in_telegram(self, chat_id: int):
        """
        Retrieves the earliest message (Telethon): limit=1, reverse=True, offset_id=0
        """
        try:
            msgs = await self.client.get_messages(chat_id, limit=1, offset_id=0, reverse=True)
            if msgs:
                return msgs[0].id
            return None
        except Exception as e:
            logger.warning(f"[GapsManager] _get_earliest_in_telegram({chat_id}) error: {e}")
            return None

    async def handle_gap_scan_response(self, data: dict):
        """
        Called from kafka_consumer when a message of type=gap_scan_response is received.
        Finds the correlation_id, resolves the future.
        """
        correlation_id = data.get("correlation_id")
        if not correlation_id:
            logger.warning("[GapsManager] gap_scan_response without correlation_id?")
            return

        task_info = self.pending_tasks.pop(correlation_id, None)
        if not task_info:
            logger.warning(f"[GapsManager] Did not find pending_task for correlation_id={correlation_id}")
            return

        fut = task_info["future"]
        if not fut.done():
            fut.set_result(data)
        else:
            logger.debug(f"[GapsManager] Future already done for correlation_id={correlation_id}")
