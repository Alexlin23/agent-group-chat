"""AgentWorker — independent agent processing unit for chat mode.

Each agent runs as its own async task, reading from MessageBus
and writing back when done. No dependency on other workers.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

from event_buffer import EventBuffer
from hermes_client import stream_hermes
from text_utils import (
    build_context_text,
    strip_agent_prefix,
    extract_mentioned_agents,
    MAX_MENTION_DEPTH,
)

_SKILL_CACHE = None


def _load_project_skill() -> str:
    """Load the project skill file (cached)."""
    global _SKILL_CACHE
    if _SKILL_CACHE is not None:
        return _SKILL_CACHE
    skill_path = Path(__file__).parent / ".hermes" / "skills" / "agent-group-chat.md"
    if skill_path.exists():
        _SKILL_CACHE = skill_path.read_text(encoding="utf-8") + "\n\n"
    else:
        _SKILL_CACHE = ""
    return _SKILL_CACHE


class AgentWorker:
    """Independent agent processing unit.

    Reads from MessageBus snapshot, calls Hermes API with streaming,
    writes response back to MessageBus when done.
    """

    def __init__(
        self,
        agent_id: str,
        agents: dict,
        message_snapshot: list[dict],
        event_buffer: EventBuffer,
        hermes_url: str,
        hermes_key: str,
        task_id: str,
        depth: int = 0,
        reply_to_seq: int = 0,
    ):
        self.agent_id = agent_id
        self.agents = agents
        self.snapshot = message_snapshot
        self.event_buffer = event_buffer
        self.hermes_url = hermes_url
        self.hermes_key = hermes_key
        self.task_id = task_id
        self.depth = depth
        self.reply_to_seq = reply_to_seq
        self._cancelled = False
        self.response: str = ""
        self.mentioned_agents: list[str] = []

    async def run(self) -> Optional[dict]:
        """Execute agent processing. Returns the new message or None if cancelled."""
        if self.agent_id not in self.agents:
            return None

        agent = self.agents[self.agent_id]

        # Emit agent_start
        self.event_buffer.push("agent_start", {
            "agent_id": self.agent_id,
            "name": agent["name"],
            "task_id": self.task_id,
        })

        # Build system prompt
        context_text = build_context_text(self.snapshot, self.agents)
        skill_text = _load_project_skill()
        system_prompt = (
            f"{agent['system_prompt']}\n\n"
            f"{skill_text}"
            f"---\n以下是完整的对话历史（包含所有参与者）：\n\n{context_text}\n---\n\n"
            f"请以 [{agent['name']}] 的身份回复最后一条消息。"
            f"你的回复会自动添加到对话中，不需要加 [{agent['name']}] 前缀。"
        )

        # Stream response
        self.response = ""
        try:
            async for chunk in stream_hermes(self.hermes_url, self.hermes_key, system_prompt):
                if self._cancelled:
                    break
                self.response += chunk
                self.event_buffer.push("text", {
                    "agent_id": self.agent_id,
                    "task_id": self.task_id,
                    "text": chunk,
                })
        except RuntimeError as e:
            self.event_buffer.push("error", {
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "error": str(e),
            })
            return None

        if self._cancelled or not self.response:
            return None

        # Emit agent_done
        self.event_buffer.push("agent_done", {
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "full_response": self.response,
            "reply_to_seq": self.reply_to_seq,
        })

        # Check for @mentions
        if self.depth < MAX_MENTION_DEPTH:
            self.mentioned_agents = extract_mentioned_agents(self.response, self.agents)

        # Build the message to persist
        clean_response = strip_agent_prefix(self.response, agent["name"])
        return {
            "role": "assistant",
            "content": clean_response,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "reply_to_seq": self.reply_to_seq,
            "timestamp": datetime.now().isoformat(),
        }

    def cancel(self):
        """Cancel this worker (interrupt @mention chain)."""
        self._cancelled = True
