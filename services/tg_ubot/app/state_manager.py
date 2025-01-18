# services/tg_ubot/app/state_manager.py

import os
import json
import asyncio
import logging

logger = logging.getLogger("state_manager")


class StateManager:
    """
    Stores:
    - backfill_from_id for each chat
    - a rolling list of timestamps for new or edited messages (pop_new_messages_count)
    All data is in /app/data/state.json (mounted as a volume).
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
            logger.warning(f"[StateManager] File {self.state_file} not found, creating empty state.")
            return {}

    def _save_state(self):
        tmp_file = self.state_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp_file, self.state_file)
            logger.debug(f"[StateManager] State saved to {self.state_file}")
        except Exception as e:
            logger.exception(f"[StateManager] Error saving state: {e}")

    def get_backfill_from_id(self, chat_id: int) -> int:
        key = f"chat_{chat_id}_backfill_from_id"
        return self.state.get(key, None)

    def update_backfill_from_id(self, chat_id: int, new_val: int):
        key = f"chat_{chat_id}_backfill_from_id"
        self.state[key] = new_val
        self._save_state()

    def get_chats_needing_backfill(self) -> list[int]:
        """
        Return all chat_ids where backfill_from_id > 1
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
                        logger.warning(f"[StateManager] Invalid chat_id in key: {k}")
        return result

    def record_new_message(self):
        """
        Called whenever a new or edited message arrives.
        We'll store a timestamp for rate checks.
        """
        now = asyncio.get_event_loop().time()
        self.new_msg_timestamps.append(now)

    def pop_new_messages_count(self, interval: float) -> int:
        """
        Returns how many messages arrived in the last `interval` seconds,
        and removes timestamps older than `interval`.
        """
        now = asyncio.get_event_loop().time()
        cutoff = now - interval
        valid_times = [t for t in self.new_msg_timestamps if t > cutoff]
        count = len(valid_times)
        self.new_msg_timestamps = valid_times
        return count
