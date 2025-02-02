# services/tg_ubot/app/main.py

import asyncio
import os
import signal
import logging
from telethon import TelegramClient

from app.config import settings
from app.logger import setup_logging
from app.utils import ensure_dir
from app.kafka.producer import KafkaMessageProducer
from app.kafka.consumers import AIOKafkaMessageConsumer
from app.telegram.chat_info import get_all_chats_info
from app.telegram.handlers import register_unified_handler
from app.telegram.backfill import BackfillManager
from app.telegram.gaps import GapsManager
from app.telegram.state_manager import StateManager
from app.telegram.state import MessageCounter


logger = logging.getLogger("main")

MAX_BUFFER_SIZE = 10000

async def run_tg_ubot():
    # 1) Логгер
    setup_logging()
    ensure_dir("/app/data")
    ensure_dir("/app/logs")

    # 2) Инициализация KafkaProducer
    producer = KafkaMessageProducer()
    await producer.initialize()
    logger.info("[main] Kafka Producer initialized.")

    # 3) Инициализация Telethon
    client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    await client.start()
    if not await client.is_user_authorized():
        logger.error("Telegram client not authorized. Exiting.")
        return

    # 4) Сбор информации о чатах
    chat_id_to_data = await get_all_chats_info(client)
    logger.info(f"[main] Discovered {len(chat_id_to_data)} chats/channels after exclusions.")

    # 5) Очередь для отправки сообщений в Kafka
    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)

    # Признак включения userbot
    userbot_active = asyncio.Event()
    userbot_active.set()

    # 6) State (backfill_from_id) и счётчик сообщений
    state_mgr = StateManager("/app/data/state.json")
    msg_counter = MessageCounter(client, threshold=100)

    stop_event = asyncio.Event()

    def handle_signal(signum, frame):
        logger.warning(f"Signal {signum} received, shutting down.")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, handle_signal, s, None)
        except NotImplementedError:
            pass

    # 7) producer_task читает (topic, data) из message_buffer, шлёт в Kafka
    async def producer_task():
        while not stop_event.is_set():
            item = await message_buffer.get()
            if item is None:
                break
            topic, data = item
            if data is not None:
                # Шлём в Kafka
                await producer.send_message(topic, data)
            message_buffer.task_done()
            logger.debug("[producer_task] Sent message to Kafka")

    producer_coro = asyncio.create_task(producer_task())

    # 8) BackfillManager
    async def backfill_callback(data: dict):
        """
        Вызывается при backfill, чтобы положить data в Kafka.
        """
        topic = settings.KAFKA_PRODUCE_TOPIC
        await message_buffer.put((topic, data))

    backfill_manager = BackfillManager(
        client=client,
        state_mgr=state_mgr,
        message_callback=backfill_callback,
        chat_id_to_data=chat_id_to_data
    )
    backfill_coro = asyncio.create_task(backfill_manager.run())

    # 9) GapsManager + consumer для gap_scan_response
    gap_consumer = AIOKafkaMessageConsumer(
        topics=[settings.KAFKA_GAP_SCAN_RESPONSE_TOPIC],
        group_id="gap_scan_response_group"
    )
    await gap_consumer.initialize()

    gaps_manager = GapsManager(
        kafka_producer=producer,
        kafka_consumer=gap_consumer,
        state_mgr=state_mgr,
        client=client,
        chat_id_to_data=chat_id_to_data,
        gap_scan_request_topic=settings.KAFKA_GAP_SCAN_TOPIC,
        gap_scan_response_topic=settings.KAFKA_GAP_SCAN_RESPONSE_TOPIC
    )

    async def gap_scan_response_listener():
        """
        Слушаем gap_scan_response, передаём в gaps_manager.
        """
        async for (topic, data) in gap_consumer.listen():
            if topic == settings.KAFKA_GAP_SCAN_RESPONSE_TOPIC:
                msg_type = data.get("type")
                if msg_type == "gap_scan_response":
                    await gaps_manager.handle_gap_scan_response(data)
                elif msg_type == "init_backfill":
                    cid = data.get("chat_id")
                    if cid:
                        logger.info(f"[gap_scan_listener] init_backfill => do_chat_backfill({cid})")
                        await backfill_manager._do_chat_backfill(cid)
                else:
                    logger.debug(f"[gap_scan_listener] unknown type={msg_type}")
            else:
                logger.debug(f"[gap_scan_listener] got message from unknown topic={topic}")

    gap_listener_coro = asyncio.create_task(gap_scan_response_listener())

    # Периодическая задача — для каждого чата проверяем gap_scan
    async def gap_filler_task():
        while not stop_event.is_set():
            for c_id in chat_id_to_data.keys():
                await gaps_manager.find_and_fill_gaps_for_chat(c_id)
            await asyncio.sleep(1800)  # каждые 30 мин

    gap_filler_coro = asyncio.create_task(gap_filler_task())

    # 10) Регистрируем unified_handler для новых/редактированных сообщений
    from app.telegram.handlers import register_unified_handler
    register_unified_handler(
        client=client,
        message_buffer=message_buffer,
        userbot_active=userbot_active,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr
    )

    # Запускаем Telethon
    try:
        logger.info("tg_ubot is running. Press Ctrl+C to stop.")
        await asyncio.gather(
            client.run_until_disconnected(),
            producer_coro,
            backfill_coro,
            gap_listener_coro,
            gap_filler_coro,
            stop_event.wait()
        )
    except Exception as e:
        logger.exception("[main] Error in run_tg_ubot:", exc_info=e)
    finally:
        logger.info("Shutting down...")

        userbot_active.clear()
        backfill_manager.stop()
        stop_event.set()

        # Завершаем producer_task
        await message_buffer.put(None)
        await producer_coro

        await gap_consumer.close()
        await producer.close()
        await client.disconnect()

        logger.info("tg_ubot service terminated.")


def main():
    asyncio.run(run_tg_ubot())


if __name__ == "__main__":
    main()
