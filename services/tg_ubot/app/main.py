# services/tg_ubot/app/main.py

import asyncio
import os
import signal
from telethon import TelegramClient
from .config import settings
from .logger import setup_logging
from .kafka_producer import KafkaMessageProducer
from .backfill_manager import BackfillManager
from .state_manager import StateManager
from .chat_info import get_all_chats_info
from .handlers.unified_handler import register_unified_handler
from .utils import ensure_dir
# ИМПОРТИРУЕМ MessageCounter, чтобы считать количество сообщений (см. ваш старый код)
from .state import MessageCounter

MAX_BUFFER_SIZE = 10000
RECONNECT_INTERVAL = 10

async def run_userbot():
    """
    Главная точка входа для пользовательского бота (tg_ubot).
    Запускает Telethon-клиент, KafkaProducer, бэкфилл (BackfillManager).
    """

    # Убеждаемся, что директории logs/media существуют
    ensure_dir(settings.MEDIA_DIR)
    ensure_dir(os.path.dirname(settings.LOG_FILE))

    logger = setup_logging()

    # Инициализируем Telethon
    session_file = settings.SESSION_FILE
    client = TelegramClient(session_file, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)

    # Очередь для сообщений (topic, data) -> Kafka
    message_buffer = asyncio.Queue(maxsize=MAX_BUFFER_SIZE)

    # Создаём Kafka продьюсер
    kafka_producer = KafkaMessageProducer()

    # Event для плавного завершения (SIGINT, SIGTERM)
    shutdown_event = asyncio.Event()

    # userbot_active — ваш флаг "включён/выключён"
    userbot_active = asyncio.Event()
    userbot_active.set()

    # ваш счётчик сообщений (можно его использовать, как раньше)
    counter = MessageCounter(client)

    # Обработка сигналов Ctrl+C, SIGTERM
    def shutdown_signal_handler(signum, frame):
        logger.warning(f"Получен сигнал {signum}, завершаем работу...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        try:
            loop.add_signal_handler(s, shutdown_signal_handler, s, None)
        except NotImplementedError:
            pass

    # Менеджер состояния (хранит /app/data/state.json)
    # Подразумевается, что /app/data примонтирован из volume tg_ubot_state_volume
    state_mgr = StateManager("/app/data/state.json")

    async def producer_task():
        while not shutdown_event.is_set():
            try:
                topic, data = await message_buffer.get()
                if data is None:
                    break
                # Отправляем в Kafka
                await kafka_producer.send_message(topic, data)
                message_buffer.task_done()
            except Exception as e:
                logger.exception(f"Ошибка при отправке в Kafka: {e}")
                await asyncio.sleep(RECONNECT_INTERVAL)

    # Коллбэк для unified_handler (отправляем данные в Kafka через очередь)
    async def send_message_to_kafka(data: dict):
        topic = settings.KAFKA_UBOT_OUTPUT_TOPIC
        await message_buffer.put((topic, data))

    # Инициализируем бэкфил
    backfill_manager = BackfillManager(
        client=client,
        state_mgr=state_mgr,
        message_callback=send_message_to_kafka,
        new_msgs_threshold=5,   # если за idle_timeout >5 новых сообщений, пропускаем бэкфилл
        idle_timeout=10,
        batch_size=50,
        flood_wait_delay=60,
        max_total_wait=300
    )

    try:
        # 1. Запускаем Kafka Producer
        await kafka_producer.initialize()
        logger.info("[main] Kafka Producer инициализирован.")

        # 2. Стартуем Telethon-клиент
        await client.start()
        if not await client.is_user_authorized():
            logger.error("[main] Telegram клиент не авторизован! Останавливаемся.")
            shutdown_event.set()

        # 3. Грузим диалоги, собираем информацию о чатах
        await client.get_dialogs()
        chat_id_to_data = await get_all_chats_info(client)
        logger.info(f"[main] Собрано {len(chat_id_to_data)} чатов/каналов.")

        # 4. Регистрируем unified_handler
        # Обратите внимание: передаём userbot_active, counter, state_mgr, message_buffer
        register_unified_handler(
            client=client,
            message_buffer=message_buffer,
            counter=counter,
            userbot_active=userbot_active,
            chat_id_to_data=chat_id_to_data,
            state_mgr=state_mgr
        )

        # 5. Создаём таски: producer и backfill
        producer_coro = asyncio.create_task(producer_task())
        backfill_coro = asyncio.create_task(backfill_manager.run())

        # 6. Запускаем всё
        await asyncio.gather(
            client.run_until_disconnected(),
            producer_coro,
            backfill_coro,
            shutdown_event.wait()
        )
    except Exception as e:
        logger.exception("[main] Ошибка в run_userbot:", exc_info=e)
    finally:
        # Останавливаем бэкфилл
        backfill_manager.stop()

        # Отключаем клиента
        await client.disconnect()

        # Закрываем Kafka producer
        if kafka_producer:
            await kafka_producer.close()

        logger.info("[main] Сервис tg_ubot завершён.")


if __name__ == "__main__":
    asyncio.run(run_userbot())
