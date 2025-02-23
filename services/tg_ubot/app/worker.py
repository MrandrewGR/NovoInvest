# services/tg_ubot/app/worker.py

import asyncio
import logging

from mirco_services_data_management.base_worker import BaseWorker

from app.telegram.backfill import BackfillManager
from app.telegram.gaps import LocalGapsManager

logger = logging.getLogger("worker")

class TGUBotWorker(BaseWorker):
    """
    Minimal userbot worker that:
      - Optionally uses BaseWorker Kafka consumer/producer for other topics
      - Uses LocalGapsManager to detect missing ID ranges
      - Uses BackfillManager to fill missing + older msgs
    """

    def __init__(self, config, client, chat_id_to_data, state_mgr, message_callback):
        super().__init__(config)
        self.client = client
        self.chat_id_to_data = chat_id_to_data
        self.state_mgr = state_mgr
        self.message_callback = message_callback
        self.stop_event = asyncio.Event()

        # Backfill
        self.backfill_manager = BackfillManager(
            client=self.client,
            state_mgr=self.state_mgr,
            message_callback=self.message_callback,
            chat_id_to_data=self.chat_id_to_data
        )
        # Local Gaps
        self.gaps_manager = LocalGapsManager(
            state_mgr=self.state_mgr,
            client=self.client,
            chat_id_to_data=self.chat_id_to_data
        )

    async def start(self):
        # Start BaseWorker (sets up main Kafka consumer/producer if you want them)
        await super().start()

    async def _after_baseworker_started(self):
        # Launch tasks for backfill and local gap scanning
        asyncio.create_task(self._backfill_loop(), name="backfill_loop")
        asyncio.create_task(self._gap_finder_loop(), name="gap_finder_loop")

    async def _backfill_loop(self):
        logger.info("[TGUBotWorker] backfill_manager started.")
        await self.backfill_manager.run()
        logger.info("[TGUBotWorker] backfill_manager stopped.")

    async def _gap_finder_loop(self):
        """
        Periodically calls LocalGapsManager to detect missing ID ranges in DB.
        E.g. every 30 min. Adjust as needed.
        """
        logger.info("[TGUBotWorker] local gap_finder started.")
        try:
            while not self.stop_event.is_set():
                for chat_id in self.chat_id_to_data:
                    await self.gaps_manager.find_and_fill_gaps_for_chat(chat_id)
                await asyncio.sleep(1800)
        except asyncio.CancelledError:
            logger.info("[TGUBotWorker] local gap_finder cancelled.")
        except Exception as e:
            logger.exception(f"[TGUBotWorker] local gap_finder error: {e}")

    async def shutdown(self):
        logger.info("[TGUBotWorker] shutdown() called.")
        self.stop_event.set()
        self.backfill_manager.stop()
        await super().shutdown()

    async def handle_message(self, message: dict):
        """
        If your config.KAFKA_CONSUME_TOPIC is "my_input_topic", you can
        process them here. Or ignore if you don't need it.
        """
        logger.debug(f"[TGUBotWorker.handle_message] got message: {message}")
        # e.g. do something with the message
        pass
