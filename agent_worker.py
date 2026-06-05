"""AgentWorker — independent agent processing unit.

Each agent runs as its own async task, reading from MessageBus
and writing back when done. No dependency on other workers.

This replaces the LangGraph state graph for agent orchestration.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, AsyncGenerator

import aiohttp

from event_buffer import EventBuffer


# ── Hermes API Streaming ─────────────────────────────────────────────────────

async def stream_hermes(
    hermes_url: str, hermes_key: str, system_prompt: str
) -> AsyncGenerator[str, None]:
    """Call Hermes API Server and yield content chunks."""
    headers = {"Content-Type": "application/json"}
    if hermes_key:
        headers["Authorization"] = f"Bearer {hermes_key}"

    payload = {
        "model": "hermes-agent",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "请回复。"},
        ],
        "stream": True,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{hermes_url}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=600),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {error_text[:200]}")

                buffer = ""
                async for chunk in resp.content.iter_any():
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("data: "):
                            data_str = line[6:]
                            if data_str == "[DONE]":
                                return
                            try:
                                data = json.loads(data_str)
                                delta = data.get("choices", [{}])[0].get("delta", {})
                                content = delta.get("content", "")
                                if content:
                                    yield content
                            except json.JSONDecodeError:
                                pass

                for line in buffer.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("data: ") and line[6:] != "[DONE]":
                        try:
                            data = json.loads(line[6:])
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            pass
    except Exception as e:
        raise RuntimeError(str(e)[:200]) from e


# ── Helpers ───────────────────────────────────────────────────────────────────

def build_context_text(messages: list[dict], agents: dict) -> str:
    lines = []
    for msg in messages:
        if msg["role"] == "user":
            lines.append(f"[用户]: {msg['content']}")
        elif msg["role"] == "assistant":
            aname = agents.get(msg.get("agent_id", ""), {}).get("name", "Agent")
            lines.append(f"[{aname}]: {msg['content']}")
    return "\n\n".join(lines) if lines else "(暂无对话历史)"


def strip_agent_prefix(text: str, agent_name: str) -> str:
    for prefix in [f"[{agent_name}]:", f"[{agent_name}] :"]:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def extract_mentioned_agents(text: str, agents: dict) -> list[str]:
    if not text or not agents:
        return []
    all_tags = []
    for a in agents.values():
        all_tags.append((a["name"], a["id"]))
        all_tags.append((a["id"], a["id"]))
    all_tags.sort(key=lambda x: len(x[0]), reverse=True)

    found_ids: list[str] = []
    idx = 0
    while idx < len(text):
        if text[idx] == "@":
            matched = False
            for tag, aid in all_tags:
                end = idx + 1 + len(tag)
                if text[idx + 1:end] == tag:
                    if end >= len(text) or not (text[end].isalnum() or '\u4e00' <= text[end] <= '\u9fff'):
                        if aid not in found_ids:
                            found_ids.append(aid)
                        idx = end
                        matched = True
                        break
            if not matched:
                idx += 1
        else:
            idx += 1
    return found_ids


# ── AgentWorker ───────────────────────────────────────────────────────────────

MAX_MENTION_DEPTH = 1000
MAX_RESPONSES_PER_AGENT = 3


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
    ):
        self.agent_id = agent_id
        self.agents = agents
        self.snapshot = message_snapshot
        self.event_buffer = event_buffer
        self.hermes_url = hermes_url
        self.hermes_key = hermes_key
        self.task_id = task_id
        self.depth = depth
        self._cancelled = False
        self.response: str = ""
        self.mentioned_agents: list[str] = []

    async def run(self) -> dict | None:
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
        system_prompt = (
            f"{agent['system_prompt']}\n\n"
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
            "timestamp": datetime.now().isoformat(),
        }

    def cancel(self):
        """Cancel this worker (interrupt @mention chain)."""
        self._cancelled = True
