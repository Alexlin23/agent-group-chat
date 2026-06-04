"""EventBuffer — per-conversation event log with SSE subscription.

Each conversation gets one EventBuffer. Agent processing pushes events here.
SSE endpoints subscribe and stream events to the frontend.

Supports replay: a new subscriber can start from any event ID and get all
past events plus new ones as they arrive. This is what makes refresh-safe
streaming possible.
"""

import asyncio
import json
import time
from typing import AsyncGenerator


class EventBuffer:
    """In-memory event log with async subscription."""

    def __init__(self):
        self.events: list[dict] = []
        self._waiters: list[asyncio.Event] = []
        self._closed = False

    def push(self, event_type: str, data: dict | None = None) -> int:
        """Append an event. Returns the event ID."""
        event_id = len(self.events)
        event = {"id": event_id, "type": event_type, "ts": time.time()}
        if data:
            event.update(data)
        self.events.append(event)
        # Wake all subscribers
        for w in self._waiters:
            w.set()
        return event_id

    def get_last_id(self) -> int:
        """Return the ID of the last event, or -1 if empty."""
        return len(self.events) - 1

    async def subscribe(self, last_id: int = -1) -> AsyncGenerator[dict, None]:
        """Yield events starting from last_id+1.

        Blocks when caught up, yields new events as they arrive.
        Sends a heartbeat every 30s if no new events.
        Yields None (stops) when the buffer is closed.
        """
        idx = last_id + 1
        while not self._closed:
            # Drain all available events
            while idx < len(self.events):
                yield self.events[idx]
                idx += 1

            if self._closed:
                break

            # Wait for new events
            waiter = asyncio.Event()
            self._waiters.append(waiter)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                # Heartbeat to keep connection alive
                yield {"type": "_heartbeat", "id": -1, "ts": time.time()}
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

    def close(self):
        """Mark buffer as done. Subscribers will drain remaining events and stop."""
        self._closed = True
        for w in self._waiters:
            w.set()

    @property
    def is_active(self) -> bool:
        """True if there might be more events coming."""
        return not self._closed


def format_sse(event: dict) -> str:
    """Format an event dict as an SSE string for StreamingResponse."""
    eid = event.get("id", 0)
    etype = event.get("type", "message")
    # Don't send internal events
    if etype.startswith("_"):
        return ""
    data = json.dumps(event, ensure_ascii=False)
    lines = []
    if eid >= 0:
        lines.append(f"id: {eid}")
    lines.append(f"event: {etype}")
    lines.append(f"data: {data}")
    return "\n".join(lines) + "\n\n"
