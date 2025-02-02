# services/tg_ubot/app/telegram/state_manager.py

"""
StateManager хранит:
 - backfill_from_id для каждого чата
 - временные метки последних сообщений (для принятия решения о backfill)
"""

import os
import json
import asyncio
import logging

logger = logging.getLogger("state_manager")


class StateManager:
    """
    Позволяет:
     - update_backfill_from_id(chat_id, val)
     - get_backfill_from_id(chat_id)
     - record_new_message() => регистрируем новое/отредактированное сообщение
     - pop_new_messages_count(interval) => сколько сообщений было за последние 'interval' секунд
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
                logger.info(f"[StateManager] Loaded state from {self.state_file}")
                return data
            except Exception as e:
                logger.exception(f"[StateManager] Could not load {self.state_file}: {e}")
                return {}
        else:
            logger.warning(f"[StateManager] {self.state_file} not found, creating empty state.")
            return {}

    def _save_state(self):
        tmp_file = self.state_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, self.state_file)
            logger.debug(f"[StateManager] Saved state to {self.state_file}")
        except Exception as e:
            logger.exception(f"[StateManager] Error saving state: {e}")

    def get_backfill_from_id(self, chat_id: int):
        key = f"chat_{chat_id}_backfill_from_id"
        return self.state.get(key, None)

    def update_backfill_from_id(self, chat_id: int, new_val: int):
        key = f"chat_{chat_id}_backfill_from_id"
        self.state[key] = new_val
        self._save_state()

    def get_chats_needing_backfill(self):
        """
        Возвращает все chat_id, у которых backfill_from_id>1
        """
        result = []
        for k, v in self.state.items():
            if k.endswith("_backfill_from_id"):
                if isinstance(v, int) and v > 1:
                    try:
                        chat_str = k.replace("chat_", "").replace("_backfill_from_id", "")
                        cid = int(chat_str)
                        result.append(cid)
                    except ValueError:
                        logger.warning(f"[StateManager] Invalid chat id in key: {k}")
        return result

    def record_new_message(self):
        """
        Вызывается при каждом новом/отредактированном сообщении: сохраняем таймштамп.
        """
        now = asyncio.get_event_loop().time()
        self.new_msg_timestamps.append(now)

    def pop_new_messages_count(self, interval: float) -> int:
        """
        Смотрим, сколько сообщений было за последние interval секунд,
        удаляем старые таймштампы.
        """
        now = asyncio.get_event_loop().time()
        cutoff = now - interval
        valid_times = [t for t in self.new_msg_timestamps if t > cutoff]
        count = len(valid_times)
        self.new_msg_timestamps = valid_times
        return count
