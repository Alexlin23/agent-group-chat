"""LangGraph state graph for multi-agent orchestration.

The graph:
  1. Takes a user message, parses @mentions to build the agent queue
  2. Processes agents one by one (calling Hermes API with streaming)
  3. After each agent, checks for @mentions in the response
  4. Loops back if new agents were mentioned, or finishes

Each agent's response is streamed token-by-token to the EventBuffer,
which the SSE endpoint relays to the frontend in real time.
"""

import operator
from datetime import datetime
from pathlib import Path
from typing import Any, Annotated, TypedDict

from langgraph.graph import END, StateGraph

from hermes_client import stream_hermes
from text_utils import (
    build_context_text,
    build_peer_descriptions,
    strip_agent_prefix,
    extract_mentioned_agents,
    MAX_MENTION_DEPTH,
)

MAX_RESPONSES_PER_AGENT = 3

_SKILL_CACHE = None


def _load_project_skill() -> str:
    global _SKILL_CACHE
    if _SKILL_CACHE is not None:
        return _SKILL_CACHE
    skill_path = Path(__file__).parent / ".hermes" / "skills" / "agent-group-chat.md"
    if skill_path.exists():
        _SKILL_CACHE = skill_path.read_text(encoding="utf-8") + "\n\n"
    else:
        _SKILL_CACHE = ""
    return _SKILL_CACHE


# ── State ─────────────────────────────────────────────────────────────────────

class ChatState(TypedDict):
    conv_id: str
    agent_queue: list[tuple[str, int]]
    agent_response_count: dict[str, int]
    messages: Annotated[list[dict], operator.add]
    event_buffer: Any
    agents_ref: dict[str, dict]
    hermes_url: str
    hermes_key: str
    new_messages: Annotated[list[dict], operator.add]


# ── Graph Nodes ───────────────────────────────────────────────────────────────

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
    start_time = datetime.now().isoformat()

    # Emit agent_start
    task_id = f"{state['conv_id']}_{agent_id}_{depth}"
    event_buffer.push("agent_start", {
        "agent_id": agent_id,
        "name": agent["name"],
        "task_id": task_id,
        "timestamp": start_time,
    })

    # Build system prompt with full conversation context
    context_text = build_context_text(state["messages"], agents)
    skill_text = _load_project_skill()
    peer_text = build_peer_descriptions(agent_id, agents)
    system_prompt = (
        f"{agent['system_prompt']}\n\n"
        f"{skill_text}\n\n"
        f"{peer_text}\n\n"
        f"---\n以下是完整的对话历史（包含所有参与者）：\n\n{context_text}\n---\n\n"
        f"请以 [{agent['name']}] 的身份回复最后一条消息。"
        f"你的回复会自动添加到对话中，不需要加 [{agent['name']}] 前缀。"
    )

    # Stream response from Hermes API
    full_response = ""
    try:
        async for chunk in stream_hermes(
            state["hermes_url"], state["hermes_key"], system_prompt,
        ):
            full_response += chunk
            event_buffer.push("text", {
                "agent_id": agent_id,
                "task_id": task_id,
                "text": chunk,
            })
    except RuntimeError as e:
        event_buffer.push("error", {
            "agent_id": agent_id,
            "task_id": task_id,
            "error": str(e),
        })
        return {"agent_queue": [], "new_messages": []}

    # Emit agent_done
    event_buffer.push("agent_done", {
        "agent_id": agent_id,
        "task_id": task_id,
        "full_response": full_response,
        "timestamp": start_time,
    })

    # Clean and store the response
    clean_response = strip_agent_prefix(full_response, agent["name"])
    new_message = {
        "role": "assistant",
        "content": clean_response,
        "agent_id": agent_id,
        "task_id": task_id,
        "timestamp": start_time,
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
