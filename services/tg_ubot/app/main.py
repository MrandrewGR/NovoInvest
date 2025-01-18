# services/tg_ubot/app/main.py

import asyncio
import os
import signal
from telethon import TelegramClient
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .kafka_consumer import KafkaMessageConsumer
from .backfill_manager import BackfillManager
from .state_manager import StateManager
from .chat_info import get_all_chats_info
from .handlers.unified_handler import register_unified_handler
from .utils import ensure_dir
from .state import MessageCounter
from .gaps_manager import GapsManager

MAX_BUFFER_SIZE = 10000
RECONNECT_INTERVAL = 10


async def run_userbot():
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))
    logger = setup_logging()

    session_file = settings.SESSION_FILE
    client = TelegramClient(session_file, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)
    kafka_producer = KafkaMessageProducer()
    shutdown_event = asyncio.Event()

    userbot_active = asyncio.Event()
    userbot_active.set()

    counter = MessageCounter(client)

    def shutdown_signal_handler(signum, frame):
        logger.warning(f"Получен сигнал {signum}, завершаем работу...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        try:
            loop.add_signal_handler(s, shutdown_signal_handler, s, None)
        except NotImplementedError:
            pass

    state_mgr = StateManager("/app/data/state.json")

    async def producer_task():
        while not shutdown_event.is_set():
            try:
                topic, data = await message_buffer.get()
                if data is None:
                    break
                await kafka_producer.send_message(topic, data)
                message_buffer.task_done()
            except Exception as e:
                logger.exception(f"Ошибка при отправке в Kafka: {e}")
                await asyncio.sleep(RECONNECT_INTERVAL)

    async def send_message_to_kafka(data: dict):
        topic = settings.KAFKA_UBOT_OUTPUT_TOPIC
        await message_buffer.put((topic, data))

    # Инициализируем KafkaProducer
    await kafka_producer.initialize()
    logger.info("[main] Kafka Producer инициализирован.")

    # Инициализируем Telethon
    await client.start()
    if not await client.is_user_authorized():
        logger.error("[main] Telegram клиент не авторизован! Останавливаемся.")
        shutdown_event.set()

    await client.get_dialogs()
    chat_id_to_data = await get_all_chats_info(client)
    logger.info(f"[main] Собрано {len(chat_id_to_data)} чатов/каналов.")

    # Регистрируем unified_handler
    register_unified_handler(
        client=client,
        message_buffer=message_buffer,
        counter=counter,
        userbot_active=userbot_active,
        chat_id_to_data=chat_id_to_data,
        state_mgr=state_mgr
    )

    # ---------------------------
    # Добавляем Kafka consumer, который слушает gap_scan_response
    # ---------------------------
    gap_scan_response_topic = os.environ.get("KAFKA_GAP_SCAN_RESPONSE_TOPIC", "gap_scan_response")
    kafka_consumer = KafkaMessageConsumer(
        topics=[gap_scan_response_topic],
        group_id="gap_scan_response_group"
    )
    await kafka_consumer.initialize()
    logger.info("[main] KafkaMessageConsumer для gap_scan_response инициализирован.")

    # Создаём GapsManager
    gaps_manager = GapsManager(
        kafka_producer=kafka_producer,
        kafka_consumer=kafka_consumer,  # чтобы зарегистрировать handle_gap_scan_response
        state_mgr=state_mgr,
        client=client,
        chat_id_to_data=chat_id_to_data,  # Передаём chat_id_to_data
        gap_scan_request_topic="gap_scan_request",
        gap_scan_response_topic=gap_scan_response_topic
    )

    # Создаём BackfillManager
    backfill_manager = BackfillManager(
        client=client,
        state_mgr=state_mgr,
        message_callback=send_message_to_kafka,
        chat_id_to_data=chat_id_to_data,  # Передаём chat_id_to_data
        new_msgs_threshold=5,
        idle_timeout=10,
        batch_size=50,
        flood_wait_delay=60,
        max_total_wait=300
    )

    # Слушатель gap_scan_response
    async def gap_scan_response_listener():
        async for (topic, data) in kafka_consumer.listen():
            if topic == gap_scan_response_topic:
                if data.get("type") == "gap_scan_response":
                    # вызываем gaps_manager.handle_gap_scan_response
                    await gaps_manager.handle_gap_scan_response(data)
                elif data.get("type") == "init_backfill":
                    # Инициируем бэкфилл для данного чата
                    chat_id = data.get("chat_id")
                    if chat_id:
                        logger.info(f"[main] Инициируем бэкфилл для chat_id={chat_id}")
                        await backfill_manager._do_chat_backfill(chat_id)
                else:
                    logger.debug(f"[gap_scan_response_listener] Получено другое сообщение {data}")
            else:
                logger.debug(f"[gap_scan_response_listener] Пришло из неизвестного топика: {topic}")

    # Пример корутины, которая раз в полчаса сканирует пропуски
    async def gap_filler_task():
        while not shutdown_event.is_set():
            for c_id in chat_id_to_data.keys():
                await gaps_manager.find_and_fill_gaps_for_chat(c_id)
            await asyncio.sleep(1800)  # каждые 30 минут

    # Запускаем BackfillManager
    backfill_coro = asyncio.create_task(backfill_manager.run())

    # Запускаем остальные задачи
    producer_coro = asyncio.create_task(producer_task())
    gap_listener_coro = asyncio.create_task(gap_scan_response_listener())
    gap_filler_coro = asyncio.create_task(gap_filler_task())

    try:
        await asyncio.gather(
            client.run_until_disconnected(),
            producer_coro,
            backfill_coro,
            gap_listener_coro,
            gap_filler_coro,
            shutdown_event.wait()
        )
    except Exception as e:
        logger.exception("[main] Ошибка в run_userbot:", exc_info=e)
    finally:
        backfill_manager.stop()
        await client.disconnect()
        if kafka_producer:
            await kafka_producer.close()
        await kafka_consumer.close()
        logger.info("[main] Сервис tg_ubot завершён.")


# Вызов асинхронной функции через asyncio.run()
if __name__ == "__main__":
    asyncio.run(run_userbot())
