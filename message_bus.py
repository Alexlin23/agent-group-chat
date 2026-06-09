"""MessageBus — shared conversation state.

All agents read from the same message list. Writes are atomic
(one asyncio lock protects file persistence only, not reads).

This replaces the direct conv["messages"] manipulation in server.py.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional


class MessageBus:
    """Per-conversation shared message state."""

    def __init__(self, conv_id: str, data_dir: Path, initial_messages: list[dict] = None):
        self.conv_id = conv_id
        self.data_dir = data_dir
        self.messages: list[dict] = list(initial_messages or [])
        self._write_lock = asyncio.Lock()
        self._name = conv_id
        self._created_at = datetime.now().isoformat()

        # Try to load metadata from existing file
        fp = data_dir / f"{conv_id}.json"
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._name = data.get("name", conv_id)
                self._created_at = data.get("created_at", self._created_at)
            except (json.JSONDecodeError, KeyError):
                pass

    def get_snapshot(self) -> list[dict]:
        """Get a copy of current messages (for agent workers)."""
        return list(self.messages)

    def get_latest(self, since_index: int) -> list[dict]:
        """Get messages after the given index."""
        return self.messages[since_index:]

    def message_count(self) -> int:
        return len(self.messages)

    async def append(self, msg: dict):
        """Atomically append a message."""
        async with self._write_lock:
            msg.setdefault("timestamp", datetime.now().isoformat())
            self.messages.append(msg)
            await self._persist()

    async def append_batch(self, msgs: list[dict]):
        """Atomically append multiple messages, then sort by timestamp."""
        async with self._write_lock:
            for msg in msgs:
                msg.setdefault("timestamp", datetime.now().isoformat())
                self.messages.append(msg)
            self._sort_by_order()
            await self._persist()

    def _sort_by_order(self):
        """Sort messages by timestamp (ISO 8601 lexicographic)."""
        self.messages.sort(key=lambda m: m.get("timestamp", ""))

    async def _persist(self):
        """Write messages to JSON file. Called under _write_lock."""
        fp = self.data_dir / f"{self.conv_id}.json"
        conv_data = {
            "id": self.conv_id,
            "name": self._name,
            "created_at": self._created_at,
            "messages": self.messages,
        }
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(conv_data, f, ensure_ascii=False, indent=2)


class MessageBusManager:
    """Manages MessageBus instances for all conversations."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._buses: dict[str, MessageBus] = {}
        self._lock = asyncio.Lock()

    def get(self, conv_id: str) -> Optional[MessageBus]:
        """Get an existing bus (no creation)."""
        return self._buses.get(conv_id)

    async def get_or_create(self, conv_id: str, initial_messages: list[dict] = None) -> MessageBus:
        """Get or create a bus for a conversation."""
        if conv_id in self._buses:
            return self._buses[conv_id]
        async with self._lock:
            if conv_id in self._buses:
                return self._buses[conv_id]
            bus = MessageBus(conv_id, self.data_dir, initial_messages)
            self._buses[conv_id] = bus
            return bus

    def remove(self, conv_id: str):
        """Remove a bus (when conversation is deleted)."""
        self._buses.pop(conv_id, None)
