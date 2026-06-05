"""Agent Group Chat Server v2 — modular architecture.

Architecture:
  - POST /message starts a background task, returns immediately
  - GET /stream provides SSE with auto-replay (refresh-safe streaming)
  - EventBuffer decouples agent processing from HTTP connections
  - Per-conversation asyncio.Lock prevents concurrent task conflicts
"""

import asyncio
import os
from pathlib import Path
from typing import Optional

import aiohttp
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.middleware.cors import CORSMiddleware

from event_buffer import EventBuffer
from message_bus import MessageBusManager
from orchestrator import Orchestrator
from storage import load_agents, load_conversations
from task_flow import TaskFlowManager

# ── Config ────────────────────────────────────────────────────────────────────
HERMES_API_URL = os.getenv("HERMES_API_URL", "http://127.0.0.1:8642")
HERMES_API_KEY = os.getenv("HERMES_API_KEY", os.getenv("API_SERVER_KEY", ""))
SERVER_PORT = int(os.getenv("CHAT_SERVER_PORT", "8081"))

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "conversations"
AGENTS_FILE = BASE_DIR / "agents.yaml"
FLOWS_DIR = BASE_DIR / "task_flows"
RUNS_DIR = BASE_DIR / "task_flow_runs"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Agent Group Chat v2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Shared State ──────────────────────────────────────────────────────────────
agents: dict[str, dict] = {}
conversations: dict[str, dict] = {}

active_tasks: dict[str, asyncio.Task] = {}
active_buffers: dict[str, EventBuffer] = {}
conversation_locks: dict[str, asyncio.Lock] = {}
bus_manager = MessageBusManager(DATA_DIR)
orchestrator: Optional[Orchestrator] = None
task_flow_manager: Optional[TaskFlowManager] = None


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global agents, conversations, orchestrator, task_flow_manager
    agents = load_agents(AGENTS_FILE)
    conversations = load_conversations(DATA_DIR)
    for cid, conv in conversations.items():
        await bus_manager.get_or_create(cid, conv.get("messages", []))
    orchestrator = Orchestrator(
        agents=agents,
        hermes_url=HERMES_API_URL,
        hermes_key=HERMES_API_KEY,
        max_concurrent=3,
    )
    # Initialize Task Flow manager
    task_flow_manager = TaskFlowManager(
        flows_dir=FLOWS_DIR,
        runs_dir=RUNS_DIR,
        agents=agents,
        hermes_url=HERMES_API_URL,
        hermes_key=HERMES_API_KEY,
    )
    task_flow_manager.load()
    # Reset stale streaming flags
    from storage import save_conversation as _save_conv
    for conv in conversations.values():
        if conv.get("is_streaming"):
            conv["is_streaming"] = False
            _save_conv(DATA_DIR, conv)
    print(f"Loaded {len(agents)} agents, {len(conversations)} conversations")
    print(f"Task Flows: {len(task_flow_manager.flows)}")
    print(f"Hermes API: {HERMES_API_URL}")
    print(f"Server running on port {SERVER_PORT}")


# ── Register Routes ───────────────────────────────────────────────────────────

from routes.agents import router as agents_router
from routes.conversations import router as conversations_router
from routes.task_flows import router as task_flows_router

app.include_router(agents_router)
app.include_router(conversations_router)
app.include_router(task_flows_router)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
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
        "task_flows": len(task_flow_manager.flows) if task_flow_manager else 0,
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
