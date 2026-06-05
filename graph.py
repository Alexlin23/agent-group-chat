"""LangGraph state graph for multi-agent orchestration.

The graph:
  1. Takes a user message, parses @mentions to build the agent queue
  2. Processes agents one by one (calling Hermes API with streaming)
  3. After each agent, checks for @mentions in the response
  4. Loops back if new agents were mentioned, or finishes

Each agent's response is streamed token-by-token to the EventBuffer,
which the SSE endpoint relays to the frontend in real time.
"""

import json
import operator
import re
from datetime import datetime
from typing import Any, AsyncGenerator, TypedDict

import aiohttp
from langgraph.graph import END, StateGraph
from typing import Annotated


# ── State ─────────────────────────────────────────────────────────────────────

class ChatState(TypedDict):
    conv_id: str
    # agent_queue: REPLACED each step (default = replace for non-annotated lists)
    agent_queue: list[tuple[str, int]]
    agent_response_count: dict[str, int]
    # messages: APPENDED each step (Annotated with operator.add)
    messages: Annotated[list[dict], operator.add]
    event_buffer: Any  # EventBuffer instance (reference, not serialized)
    agents_ref: dict[str, dict]
    hermes_url: str
    hermes_key: str
    # new_messages: APPENDED — messages produced by this run
    new_messages: Annotated[list[dict], operator.add]


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


# ── Graph Nodes ───────────────────────────────────────────────────────────────

MAX_MENTION_DEPTH = 1000
MAX_RESPONSES_PER_AGENT = 3


async def process_agent(state: ChatState) -> dict:
    """Process one agent from the queue."""
    queue = state["agent_queue"]
    if not queue:
        return {}

    agent_id, depth = queue[0]
    remaining_queue = queue[1:]

    agents = state["agents_ref"]
    event_buffer = state["event_buffer"]

    if agent_id not in agents:
        return {"agent_queue": remaining_queue}

    agent = agents[agent_id]
    response_count = dict(state["agent_response_count"])
    response_count[agent_id] = response_count.get(agent_id, 0) + 1

    # Emit agent_start
    event_buffer.push("agent_start", {"agent_id": agent_id, "name": agent["name"]})

    # Build system prompt with full conversation context
    context_text = build_context_text(state["messages"], agents)
    system_prompt = (
        f"{agent['system_prompt']}\n\n"
        f"---\n以下是完整的对话历史（包含所有参与者）：\n\n{context_text}\n---\n\n"
        f"请以 [{agent['name']}] 的身份回复最后一条消息。"
        f"你的回复会自动添加到对话中，不需要加 [{agent['name']}] 前缀。"
    )

    # Stream response from Hermes API
    full_response = ""
    try:
        async for chunk in stream_hermes(state["hermes_url"], state["hermes_key"], system_prompt):
            full_response += chunk
            event_buffer.push("text", {"agent_id": agent_id, "text": chunk})
    except RuntimeError as e:
        event_buffer.push("error", {"agent_id": agent_id, "error": str(e)})
        return {"agent_queue": [], "new_messages": []}

    # Emit agent_done
    event_buffer.push("agent_done", {"agent_id": agent_id, "full_response": full_response})

    # Clean and store the response
    clean_response = strip_agent_prefix(full_response, agent["name"])
    new_message = {
        "role": "assistant",
        "content": clean_response,
        "agent_id": agent_id,
        "timestamp": datetime.now().isoformat(),
    }

    # Check for @mentions in the response
    new_queue = list(remaining_queue)
    if depth < MAX_MENTION_DEPTH and response_count[agent_id] < MAX_RESPONSES_PER_AGENT:
        mentioned = extract_mentioned_agents(full_response, agents)
        for mid in mentioned:
            if mid in agents:
                new_queue.append((mid, depth + 1))
                event_buffer.push("mention_trigger", {
                    "from_agent": agent_id,
                    "to_agent": mid,
                    "to_name": agents[mid]["name"],
                })

    # Return partial state update:
    # - messages: APPEND (Annotated with operator.add)
    # - agent_queue: REPLACE (default)
    # - agent_response_count: REPLACE
    # - new_messages: APPEND
    return {
        "agent_queue": new_queue,
        "agent_response_count": response_count,
        "messages": [new_message],
        "new_messages": [new_message],
    }


def should_continue(state: ChatState) -> str:
    queue = state["agent_queue"]
    agents = state["agents_ref"]
    response_count = state["agent_response_count"]

    valid = [
        (aid, d) for aid, d in queue
        if aid in agents and response_count.get(aid, 0) < MAX_RESPONSES_PER_AGENT
    ]
    if valid:
        return "process_agent"
    return "finish"


async def finish_node(state: ChatState) -> dict:
    event_buffer = state["event_buffer"]
    event_buffer.push("done", {})
    event_buffer.close()
    return {}


# ── Graph Builder ─────────────────────────────────────────────────────────────

def build_graph():
    """Build and compile the LangGraph state graph."""
    graph = StateGraph(ChatState)

    graph.add_node("process_agent", process_agent)
    graph.add_node("finish", finish_node)

    graph.set_entry_point("process_agent")
    graph.add_conditional_edges("process_agent", should_continue, {
        "process_agent": "process_agent",
        "finish": "finish",
    })
    graph.add_edge("finish", END)

    return graph.compile()


# ── Public Interface ──────────────────────────────────────────────────────────

_compiled_graph = None


def get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_graph()
    return _compiled_graph


async def run_agent_task(
    conv_id: str,
    user_message: str,
    target_ids: list[str],
    agents: dict,
    existing_messages: list[dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer,
) -> list[dict]:
    """Run the agent graph for a user message. Returns new messages produced."""
    graph = get_graph()

    initial_state: ChatState = {
        "conv_id": conv_id,
        "agent_queue": [(tid, 0) for tid in target_ids],
        "agent_response_count": {},
        "messages": existing_messages,
        "event_buffer": event_buffer,
        "agents_ref": agents,
        "hermes_url": hermes_url,
        "hermes_key": hermes_key,
        "new_messages": [],
    }

    try:
        result = await graph.ainvoke(initial_state)
        return result.get("new_messages", [])
    except Exception as e:
        event_buffer.push("error", {"error": str(e)[:300]})
        event_buffer.close()
        return []
