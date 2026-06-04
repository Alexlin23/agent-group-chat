"""Agent Group Chat Server v2 — LangGraph + EventBuffer.

Architecture:
  - POST /message starts a background LangGraph task, returns immediately
  - GET /stream provides SSE with auto-replay (refresh-safe streaming)
  - EventBuffer decouples agent processing from HTTP connections
  - Per-conversation asyncio.Lock prevents concurrent task conflicts
"""

import asyncio
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

from event_buffer import EventBuffer, format_sse
from graph import run_agent_task
from message_bus import MessageBus, MessageBusManager

# ── Config ────────────────────────────────────────────────────────────────────
HERMES_API_URL = os.getenv("HERMES_API_URL", "http://127.0.0.1:8642")
HERMES_API_KEY = os.getenv("HERMES_API_KEY", os.getenv("API_SERVER_KEY", ""))
SERVER_PORT = int(os.getenv("CHAT_SERVER_PORT", "8081"))

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "conversations"
AGENTS_FILE = BASE_DIR / "agents.yaml"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Agent Group Chat v2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Models ────────────────────────────────────────────────────────────────────

class AgentCreate(BaseModel):
    id: Optional[str] = None
    name: str
    color: str = "#888888"
    avatar: str = "🤖"
    system_prompt: str = ""

class AgentUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    avatar: Optional[str] = None
    system_prompt: Optional[str] = None

class ConversationCreate(BaseModel):
    name: str = "新对话"

class ConversationUpdate(BaseModel):
    name: Optional[str] = None

class MessageRequest(BaseModel):
    content: str
    targets: list[str] = []

# ── Storage ───────────────────────────────────────────────────────────────────

agents: dict[str, dict] = {}
conversations: dict[str, dict] = {}

# Per-conversation runtime state
active_tasks: dict[str, asyncio.Task] = {}       # conv_id → running asyncio.Task
active_buffers: dict[str, EventBuffer] = {}       # conv_id → current EventBuffer
conversation_locks: dict[str, asyncio.Lock] = {}  # conv_id → lock
bus_manager = MessageBusManager(DATA_DIR)         # shared message state


def _get_lock(conv_id: str) -> asyncio.Lock:
    if conv_id not in conversation_locks:
        conversation_locks[conv_id] = asyncio.Lock()
    return conversation_locks[conv_id]


def _load_agents() -> dict[str, dict]:
    if not AGENTS_FILE.exists():
        return {}
    with open(AGENTS_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    result = {}
    for a in data.get("agents", []):
        aid = a["id"]
        result[aid] = {
            "id": aid,
            "name": a.get("name", aid),
            "color": a.get("color", "#888888"),
            "avatar": a.get("avatar", "🤖"),
            "system_prompt": a.get("system_prompt", ""),
        }
    return result


def _save_agents():
    data = {"agents": list(agents.values())}
    with open(AGENTS_FILE, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def _load_conversations() -> dict[str, dict]:
    result = {}
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        return result
    for fp in DATA_DIR.glob("*.json"):
        with open(fp, "r", encoding="utf-8") as f:
            conv = json.load(f)
            result[conv["id"]] = conv
    return result


def _save_conversation(conv: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fp = DATA_DIR / f"{conv['id']}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(conv, f, ensure_ascii=False, indent=2)


def _delete_conversation_file(conv_id: str):
    fp = DATA_DIR / f"{conv_id}.json"
    if fp.exists():
        fp.unlink()


# ── @mention Parsing ──────────────────────────────────────────────────────────

def parse_mentions(text: str, agent_list: list[dict]) -> tuple[list[str], str]:
    """Parse @mentions from text. Returns (matched_agent_ids, cleaned_text)."""
    name_to_id = {}
    for a in agent_list:
        name_to_id[a["name"]] = a["id"]
        name_to_id[a["id"]] = a["id"]

    pattern = r"@(\S+)"
    found_ids: list[str] = []
    for match in re.finditer(pattern, text):
        tag = match.group(1)
        if tag in name_to_id and name_to_id[tag] not in found_ids:
            found_ids.append(name_to_id[tag])

    if not found_ids:
        return [], text

    cleaned = text
    for match in reversed(list(re.finditer(pattern, text))):
        tag = match.group(1)
        if tag in name_to_id:
            cleaned = cleaned[:match.start()] + cleaned[match.end():]
    cleaned = cleaned.strip()

    return found_ids, cleaned


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global agents, conversations
    agents = _load_agents()
    conversations = _load_conversations()
    # Initialize MessageBus for each conversation
    for cid, conv in conversations.items():
        await bus_manager.get_or_create(cid, conv.get("messages", []))
    # Reset any stale streaming flags from previous runs
    for conv in conversations.values():
        if conv.get("is_streaming"):
            conv["is_streaming"] = False
            _save_conversation(conv)
    print(f"Loaded {len(agents)} agents, {len(conversations)} conversations")
    print(f"Hermes API: {HERMES_API_URL}")
    print(f"Server running on port {SERVER_PORT}")


# ── Agent API ─────────────────────────────────────────────────────────────────

@app.get("/api/agents")
async def list_agents():
    return list(agents.values())


@app.get("/api/agents/{agent_id}")
async def get_agent(agent_id: str):
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    return agents[agent_id]


@app.post("/api/agents")
async def create_agent(req: AgentCreate):
    aid = req.id or uuid.uuid4().hex[:8]
    if aid in agents:
        raise HTTPException(409, "Agent ID already exists")
    agent = {
        "id": aid, "name": req.name, "color": req.color,
        "avatar": req.avatar, "system_prompt": req.system_prompt,
    }
    agents[aid] = agent
    _save_agents()
    return agent


@app.put("/api/agents/{agent_id}")
async def update_agent(agent_id: str, req: AgentUpdate):
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    a = agents[agent_id]
    if req.name is not None: a["name"] = req.name
    if req.color is not None: a["color"] = req.color
    if req.avatar is not None: a["avatar"] = req.avatar
    if req.system_prompt is not None: a["system_prompt"] = req.system_prompt
    _save_agents()
    return a


@app.delete("/api/agents/{agent_id}")
async def delete_agent(agent_id: str):
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    del agents[agent_id]
    _save_agents()
    return {"ok": True}


# ── Conversation API ──────────────────────────────────────────────────────────

@app.get("/api/conversations")
async def list_conversations():
    convs = sorted(conversations.values(), key=lambda c: c.get("created_at", ""), reverse=True)
    return [{
        "id": c["id"],
        "name": c["name"],
        "created_at": c.get("created_at", ""),
        "message_count": len(c.get("messages", [])),
        "is_streaming": c["id"] in active_tasks and not active_tasks[c["id"]].done(),
    } for c in convs]


@app.post("/api/conversations")
async def create_conversation(req: ConversationCreate):
    cid = uuid.uuid4().hex[:12]
    conv = {
        "id": cid, "name": req.name,
        "created_at": datetime.now().isoformat(),
        "messages": [],
    }
    conversations[cid] = conv
    _save_conversation(conv)
    await bus_manager.get_or_create(cid)
    return conv


@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: str):
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")
    return conversations[conv_id]


@app.delete("/api/conversations/{conv_id}")
async def delete_conversation(conv_id: str):
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")
    # Cancel active task if any
    if conv_id in active_tasks and not active_tasks[conv_id].done():
        active_tasks[conv_id].cancel()
    active_tasks.pop(conv_id, None)
    active_buffers.pop(conv_id, None)
    bus_manager.remove(conv_id)
    del conversations[conv_id]
    _delete_conversation_file(conv_id)
    return {"ok": True}


@app.put("/api/conversations/{conv_id}")
async def update_conversation(conv_id: str, req: ConversationUpdate):
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")
    conv = conversations[conv_id]
    if req.name is not None:
        conv["name"] = req.name
    _save_conversation(conv)
    return conv


# ── Message API (fire-and-forget + SSE stream) ───────────────────────────────

@app.post("/api/conversations/{conv_id}/message")
async def send_message(conv_id: str, req: MessageRequest):
    """Start agent processing for a message. Returns immediately."""
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")

    agent_list = list(agents.values())
    if not agent_list:
        raise HTTPException(400, "No agents configured")

    lock = _get_lock(conv_id)

    # If a task is already running, wait for it (with timeout)
    if conv_id in active_tasks and not active_tasks[conv_id].done():
        # Queue: wait for current task to finish
        try:
            await asyncio.wait_for(lock.acquire(), timeout=600)
            lock.release()
        except asyncio.TimeoutError:
            raise HTTPException(409, "Previous task still running")

    # Parse @mentions
    target_ids, cleaned_text = parse_mentions(req.content, agent_list)
    if not target_ids:
        target_ids = [a["id"] for a in agent_list]

    for tid in target_ids:
        if tid not in agents:
            raise HTTPException(400, f"Unknown agent: {tid}")

    # Store user message via MessageBus
    bus = await bus_manager.get_or_create(conv_id, conversations[conv_id].get("messages", []))
    user_msg = {
        "role": "user",
        "content": req.content,
        "agent_id": None,
    }
    await bus.append(user_msg)
    conversations[conv_id]["messages"] = bus.messages

    # Create EventBuffer and start background task
    buffer = EventBuffer()
    active_buffers[conv_id] = buffer

    async def _run():
        async with lock:
            try:
                new_messages = await run_agent_task(
                    conv_id=conv_id,
                    user_message=req.content,
                    target_ids=target_ids,
                    agents=agents,
                    existing_messages=bus.get_snapshot(),
                    hermes_url=HERMES_API_URL,
                    hermes_key=HERMES_API_KEY,
                    event_buffer=buffer,
                )
                # Persist new messages via MessageBus
                if new_messages:
                    await bus.append_batch(new_messages)
                    conversations[conv_id]["messages"] = bus.messages
            except Exception as e:
                buffer.push("error", {"error": str(e)[:300]})
                buffer.close()
            finally:
                active_buffers.pop(conv_id, None)

    task = asyncio.create_task(_run())
    active_tasks[conv_id] = task

    return {"status": "accepted", "conv_id": conv_id, "targets": target_ids}


@app.get("/api/conversations/{conv_id}/stream")
async def stream_conversation(
    conv_id: str,
    last_id: int = Query(-1, alias="last_id"),
):
    """SSE endpoint: stream events for a conversation.

    Supports replay via last_id query parameter. The frontend uses
    EventSource which auto-reconnects and sends Last-Event-Id header.
    We read it from the query param since EventSource sends it there too.
    """
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")

    buffer = active_buffers.get(conv_id)

    async def event_generator():
        if buffer is None or not buffer.is_active:
            # No active processing — send a "no_task" event and close
            yield format_sse({"type": "no_task", "id": -1})
            return

        async for event in buffer.subscribe(last_id):
            sse = format_sse(event)
            if sse:
                yield sse

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/conversations/{conv_id}/task_status")
async def task_status(conv_id: str):
    """Check if a task is running for this conversation."""
    task = active_tasks.get(conv_id)
    if task is None:
        return {"status": "idle"}
    if task.done():
        return {"status": "done"}
    return {"status": "running"}


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    import aiohttp
    hermes_ok = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{HERMES_API_URL}/health",
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                hermes_ok = resp.status == 200
    except Exception:
        pass
    return {
        "status": "ok",
        "hermes_api": hermes_ok,
        "agents": len(agents),
        "conversations": len(conversations),
        "active_tasks": sum(1 for t in active_tasks.values() if not t.done()),
    }


# ── Frontend ──────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    html_path = BASE_DIR / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print(f"Starting Agent Group Chat v2 on http://localhost:{SERVER_PORT}")
    print(f"Hermes API Server: {HERMES_API_URL}")
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, log_level="info")
