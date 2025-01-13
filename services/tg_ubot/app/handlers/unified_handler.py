# File location services/tg_ubot/app/handlers/unified_handler.py

import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.types import Message, MessageEntityUrl, MessageEntityTextUrl
from app.config import settings
from app.utils import human_like_delay, get_delay_settings, ensure_dir
import json

logger = logging.getLogger("unified_handler")


def register_unified_handler(
    client: TelegramClient,
    message_buffer: asyncio.Queue,
    counter,
    userbot_active: asyncio.Event,
    chat_id_to_data: dict,
    state_mgr=None
):
    """
    Регистрирует единый обработчик для всех "новых" и "изменённых" сообщений
    в списке целевых chat_id/каналов (settings.TELEGRAM_TARGET_IDS).

    Параметры:
    - client: Telethon-клиент
    - message_buffer: очередь (asyncio.Queue), куда отправляем (topic, data) для Kafka
    - counter: экземпляр MessageCounter (счётчик обработанных сообщений)
    - userbot_active: asyncio.Event, флаг включения/выключения юзербота
    - chat_id_to_data: словарь метаданных о чатах (результат get_all_chats_info)
    - state_mgr (необязательный): если передан, при каждом новом/редактированном
      сообщении вызывается state_mgr.record_new_message() для учёта в BackfillManager.
    """
    target_ids = settings.TELEGRAM_TARGET_IDS
    logger.info(f"Регистрируем unified_handler для чатов/каналов: {target_ids}")

    @client.on(events.NewMessage(chats=target_ids))
    async def on_new_message(event):
        # Фиксируем новое сообщение в state_mgr (если нужно для бэкфилла)
        if state_mgr is not None:
            state_mgr.record_new_message()

        # Проверяем, "включён" ли userbot
        if not userbot_active.is_set():
            return

        await process_message_event(
            event=event,
            event_type="new_message",
            message_buffer=message_buffer,
            counter=counter,
            client=client,
            chat_id_to_data=chat_id_to_data
        )

    @client.on(events.MessageEdited(chats=target_ids))
    async def on_edited_message(event):
        # Фиксируем отредактированное сообщение в state_mgr (если нужно для бэкфилла)
        if state_mgr is not None:
            state_mgr.record_new_message()

        # Проверяем, "включён" ли userbot
        if not userbot_active.is_set():
            return

        await process_message_event(
            event=event,
            event_type="edited_message",
            message_buffer=message_buffer,
            counter=counter,
            client=client,
            chat_id_to_data=chat_id_to_data
        )


async def process_message_event(event, event_type, message_buffer, counter, client, chat_id_to_data):
    """
    Обработка одного входящего или отредактированного сообщения:
      1. "Человеко-подобная" задержка (учёт день/ночь)
      2. Скачивание медиа (если есть)
      3. Сбор реакций, forward-данных, reply-информации и т.д.
      4. Формирование итогового JSON
      5. Отправка в Kafka (через message_buffer)

    Параметры:
    - event: событие Telethon
    - event_type: "new_message" или "edited_message"
    - message_buffer: очередь для отправки в Kafka
    - counter: MessageCounter (увеличиваем счётчик)
    - client: Telethon-клиент
    - chat_id_to_data: словарь метаданных о чатах
    """
    try:
        msg: Message = event.message
        logger.debug(f"[unified_handler] Получено сообщение: {msg.id} из chat_id={msg.chat_id}")

        # Проверяем, описан ли этот chat_id в chat_id_to_data
        logger.debug(f"Доступные chat_id в chat_id_to_data: {list(chat_id_to_data.keys())}")
        chat_id = msg.chat_id
        chat_info = chat_id_to_data.get(chat_id)
        if not chat_info:
            logger.warning(f"[unified_handler] chat_id={chat_id} отсутствует в chat_id_to_data. Пропускаем.")
            return

        # Задержка по дневному/ночному времени
        min_delay, max_delay = get_delay_settings("chat")
        await human_like_delay(min_delay, max_delay)

        # Увеличиваем счётчик обработанных сообщений
        await counter.increment()

        # Скачиваем медиа (если есть)
        media_path = None
        if msg.media:
            ensure_dir(settings.MEDIA_DIR)
            try:
                media_path = await msg.download_media(file=settings.MEDIA_DIR)
                logger.debug(f"[unified_handler] Медиа сохранено: {media_path}")
            except Exception as e:
                logger.error(f"[unified_handler] Не удалось скачать медиа: {e}")

        # Информация об отправителе
        sender_info = {}
        try:
            sender = await msg.get_sender()
            if sender:
                sender_info = {
                    "sender_id": getattr(sender, "id", None),
                    "sender_username": getattr(sender, "username", ""),
                    "sender_first_name": getattr(sender, "first_name", ""),
                    "sender_last_name": getattr(sender, "last_name", None),
                }
                logger.debug(f"[unified_handler] Информация об отправителе: {sender_info}")
        except Exception as e:
            logger.error(f"[unified_handler] Не удалось получить информацию об отправителе: {e}")

        # Из chat_info (собранного через get_all_chats_info)
        target_id = chat_info.get("target_id", "unknown")
        chat_title = chat_info.get("chat_title", "Untitled")

        # Сбор реакций (Telethon 1.24+)
        reactions_info = []
        if getattr(msg, "reactions", None):
            if msg.reactions.results:
                for r in msg.reactions.results:
                    emoticon = getattr(r.reaction, "emoticon", "")
                    count = r.count
                    reactions_info.append({"emoji": emoticon, "count": count})

        # Проверка reply
        reply_info = {}
        if msg.is_reply:
            try:
                reply_msg = await msg.get_reply_message()
                if reply_msg:
                    reply_info = {
                        "is_reply": True,
                        "reply_to_msg_id": reply_msg.id,
                    }
            except Exception as e:
                logger.error(f"[unified_handler] Не удалось получить reply-сообщение: {e}")

        # Проверка редактирования
        edited_info = {}
        if msg.edit_date:
            edited_info["was_edited"] = True
            edited_info["edit_date"] = msg.edit_date.isoformat()

        # Проверка forward
        forward_info = {}
        if msg.forward:
            forward_info["is_forwarded"] = True
            forward_info["forwarded_from"] = str(msg.forward.from_name or "")
            forwarded_from_id = getattr(msg.forward.from_id, "user_id", None)
            forward_info["forwarded_from_id"] = forwarded_from_id
            forward_info["forwarded_date"] = msg.forward.date.isoformat() if msg.forward.date else None
        else:
            forward_info["is_forwarded"] = False

        # Сбор «цитат» (строки, начинающиеся с '>')
        quotes = []
        raw_text = msg.raw_text or ""
        for line in raw_text.splitlines():
            if line.strip().startswith(">"):
                quotes.append(line.strip())

        # Извлечение ссылок + markdown
        text_markdown, links = build_markdown_and_links(raw_text, msg.entities or [])

        # name_or_username (из chat_info), если не задан - используем target_id
        name_or_username = chat_info.get("name_or_username")
        if not name_or_username or name_or_username == "Unknown":
            name_uname = target_id
        else:
            name_uname = name_or_username

        # Формируем итоговый JSON для Kafka
        message_data = {
            "event_type": event_type,
            "message_id": msg.id,
            "date": msg.date.isoformat(),
            "text_plain": raw_text,
            "text_markdown": text_markdown,
            "links": links,
            "media_path": media_path,
            "reactions": reactions_info,
            "quotes": quotes,
            **reply_info,
            **edited_info,
            **forward_info,
            "sender": sender_info,
            "chat_id": chat_id,
            "chat_title": chat_title,
            "target_id": target_id,
            "name_uname": name_uname,
            "month_part": msg.date.strftime("%Y-%m"),
        }

        # Логирование
        try:
            message_json = json.dumps(message_data, ensure_ascii=False)
            logger.info(f"[unified_handler] Финальный JSON для Kafka: {message_json}")
        except (TypeError, ValueError) as e:
            logger.error(f"[unified_handler] Ошибка сериализации message_data в JSON: {e}")

        # Отправляем в Kafka
        kafka_topic = settings.KAFKA_UBOT_OUTPUT_TOPIC
        await message_buffer.put((kafka_topic, message_data))
        logger.info(f"[unified_handler] Обработано сообщение {msg.id} из {chat_title}, target_id={target_id}")

    except Exception as e:
        logger.exception(f"[unified_handler] Ошибка в process_message_event: {e}")


def build_markdown_and_links(raw_text: str, entities: list):
    """
    Превращает исходный текст + entities в (markdown-текст, список_ссылок).

    Возвращает:
      ( text_markdown, links )
      где text_markdown - строка с [текстом](ссылка) в Markdown
            links - список словарей {offset, length, url, display_text}
    """
    if not entities:
        return raw_text, []

    md_fragments = []
    links = []
    last_offset = 0
    entities_sorted = sorted(entities, key=lambda e: e.offset)

    for entity in entities_sorted:
        # Копируем "промежуточный" текст
        if entity.offset > last_offset:
            md_fragments.append(raw_text[last_offset:entity.offset])

        e_length = entity.length
        display_text = raw_text[entity.offset : entity.offset + e_length]

        if isinstance(entity, MessageEntityUrl):
            # URL виден напрямую
            url = display_text
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text,
            })
            last_offset = entity.offset + e_length
        elif isinstance(entity, MessageEntityTextUrl) and entity.url:
            # Скрытая ссылка
            url = entity.url
            md_fragments.append(f"[{display_text}]({url})")
            links.append({
                "offset": entity.offset,
                "length": e_length,
                "url": url,
                "display_text": display_text,
            })
            last_offset = entity.offset + e_length
        else:
            # Остальные entity (bold, italic, mention и т.д.) - не обрабатываем отдельно
            md_fragments.append(display_text)
            last_offset = entity.offset + e_length

    # Добавляем хвост
    if last_offset < len(raw_text):
        md_fragments.append(raw_text[last_offset:])

    text_markdown = "".join(md_fragments)
    return text_markdown, links
