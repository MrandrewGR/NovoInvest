# services/tg_ubot/app/state_manager.py

import os
import json
import asyncio
import logging

logger = logging.getLogger("state_manager")

class StateManager:
    """
    Хранит:
    - last_message_id (если надо)
    - backfill_from_id (для бэкфилла)
    - список таймстемпов (для pop_new_messages_count).
    Данные лежат в файле /app/data/state.json,
    который монтируется как volume (tg_ubot_state_volume).
    """

    def __init__(self, state_file="/app/data/state.json"):
        self.state_file = state_file
        self.state = self._load_state()
        self.new_msg_timestamps = []
        self.lock = asyncio.Lock()

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"[StateManager] Состояние загружено из {self.state_file}")
                return data
            except Exception as e:
                logger.exception(f"[StateManager] Не удалось загрузить {self.state_file}: {e}")
                return {}
        else:
            logger.warning(f"[StateManager] Файл {self.state_file} не найден, создаём пустое состояние.")
            return {}

    def _save_state(self):
        tmp_file = self.state_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, self.state_file)
            logger.debug(f"[StateManager] Состояние сохранено в {self.state_file}")
        except Exception as e:
            logger.exception(f"[StateManager] Ошибка при сохранении state: {e}")

    # Методы для last_message_id (опционально)
    def get_last_message_id(self, chat_id: int) -> int:
        key = f"chat_{chat_id}_last_id"
        return self.state.get(key, 0)

    def update_last_message_id(self, chat_id: int, message_id: int):
        key = f"chat_{chat_id}_last_id"
        self.state[key] = message_id
        self._save_state()

    # Методы для backfill_from_id
    def get_backfill_from_id(self, chat_id: int) -> int:
        key = f"chat_{chat_id}_backfill_from_id"
        return self.state.get(key, None)

    def update_backfill_from_id(self, chat_id: int, new_val: int):
        key = f"chat_{chat_id}_backfill_from_id"
        self.state[key] = new_val
        self._save_state()

    def get_chats_needing_backfill(self) -> list[int]:
        """
        Ищем все чаты, у которых backfill_from_id > 1 (значит есть ещё старые сообщения).
        """
        result = []
        for k, v in self.state.items():
            if k.endswith("_backfill_from_id"):
                if isinstance(v, int) and v > 1:
                    try:
                        chat_id_str = k.replace("chat_", "").replace("_backfill_from_id", "")
                        cid = int(chat_id_str)
                        result.append(cid)
                    except ValueError:
                        logger.warning(f"[StateManager] Некорректный chat_id в ключе: {k}")
        return result

    # Учёт новых сообщений
    def record_new_message(self):
        """
        Приход нового/редактированного сообщения.
        """
        now = asyncio.get_event_loop().time()
        self.new_msg_timestamps.append(now)

    def pop_new_messages_count(self, interval: float) -> int:
        """
        Возвращает, сколько сообщений пришло за последние interval секунд,
        и удаляет старые отметки.
        """
        now = asyncio.get_event_loop().time()
        cutoff = now - interval
        valid_times = [t for t in self.new_msg_timestamps if t > cutoff]
        count = len(valid_times)
        self.new_msg_timestamps = valid_times
        return count
