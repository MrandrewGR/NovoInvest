# services/tg_ubot/app/main.py

import asyncio
import signal
import logging
from telethon import TelegramClient

from app.config import settings
from app.logger import setup_logging
from app.utils import ensure_dir
from app.telegram.chat_info import get_all_chats_info
from app.telegram.state_manager import StateManager
from app.telegram.state import MessageCounter
from app.worker import TGUBotWorker

from mirco_services_data_management.kafka_io import send_message  # если нужно отправлять сообщения в Kafka

logger = logging.getLogger("main")

async def run_tg_ubot():
    setup_logging()
    ensure_dir("/app/data")
    ensure_dir("/app/logs")

    client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    await client.start()
    if not await client.is_user_authorized():
        logger.error("Telegram client not authorized. Exiting.")
        return

    chat_id_to_data = await get_all_chats_info(client)
    logger.info(f"[main] Discovered {len(chat_id_to_data)} chats/channels after exclusions.")

    state_mgr = StateManager("/app/data/state.json")
    msg_counter = MessageCounter(client, threshold=100)

    async def message_callback(data: dict):
        """Отправляет сообщение в Kafka (если требуется), затем увеличивает счётчик обработанных сообщений
           и выводит уведомление в лог с message_id, name_uname и month_part."""
        topic = settings.UBOT_PRODUCE_TOPIC
        if worker.producer:
            await send_message(worker.producer, topic, data)
            await msg_counter.increment()
            message_id = data.get("message_id", "unknown")
            name_uname = data.get("name_uname", "unknown")
            month_part = data.get("month_part", "unknown")
            logger.debug(f"Message processed: id={message_id}, name_uname={name_uname}, month_part={month_part}")
        else:
            logger.warning("[message_callback] Producer not ready yet!")

    worker = TGUBotWorker(
        config=settings,
        client=client,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr,
        message_callback=message_callback
    )

    # Создаем очередь для новых сообщений (если она действительно нужна в process_message_event)
    message_buffer = asyncio.Queue()

    # Создаем и устанавливаем событие активности для userbot
    userbot_active = asyncio.Event()
    userbot_active.set()  # Это позволит обрабатывать новые сообщения

    # Регистрируем обработчики (передаем корректный объект для userbot_active и message_buffer)
    from app.telegram.handlers import register_unified_handler
    register_unified_handler(
        client=client,
        message_buffer=message_buffer,
        userbot_active=userbot_active,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr
    )

    stop_event = asyncio.Event()

    def handle_signal(signum, frame):
        logger.warning(f"Signal {signum} received, stopping worker.")
        stop_event.set()
        worker.stop()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, handle_signal, s, None)
        except NotImplementedError:
            pass

    async def worker_main():
        start_task = asyncio.create_task(worker.start(), name="worker_start")
        await asyncio.sleep(1)
        await worker._after_baseworker_started()
        await start_task

    worker_task = asyncio.create_task(worker_main(), name="tg_ubot_worker")
    await stop_event.wait()

    logger.info("[main] Shutting down worker...")
    worker.stop()
    worker_task.cancel()
    await asyncio.gather(worker_task, return_exceptions=True)

    await client.disconnect()
    logger.info("tg_ubot service terminated.")


def main():
    asyncio.run(run_tg_ubot())


if __name__ == "__main__":
    main()
