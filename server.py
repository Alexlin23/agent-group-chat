"""Agent Group Chat Server.

中间层：管理agent定义和对话历史，代理请求到Hermes API Server。
"""

import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

import aiohttp
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

# ── 配置 ──────────────────────────────────────────────────────────────────────
HERMES_API_URL = os.getenv("HERMES_API_URL", "http://127.0.0.1:8642")
HERMES_API_KEY = os.getenv("HERMES_API_KEY", os.getenv("API_SERVER_KEY", ""))
SERVER_PORT = int(os.getenv("CHAT_SERVER_PORT", "8080"))

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "conversations"
AGENTS_FILE = BASE_DIR / "agents.yaml"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Agent Group Chat")
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

class MessageRequest(BaseModel):
    content: str
    targets: list[str] = []

# ── Storage ───────────────────────────────────────────────────────────────────

agents: dict[str, dict] = {}
conversations: dict[str, dict] = {}


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


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global agents, conversations
    agents = _load_agents()
    conversations = _load_conversations()
    # Reset stuck streaming state from interrupted sessions
    reset_count = 0
    for conv in conversations.values():
        if conv.get("is_streaming"):
            conv["is_streaming"] = False
            _save_conversation(conv)
            reset_count += 1
    if reset_count:
        print(f"Reset {reset_count} stuck streaming conversations")
    print(f"Loaded {len(agents)} agents, {len(conversations)} conversations")
    print(f"Hermes API: {HERMES_API_URL}")


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
        "id": aid,
        "name": req.name,
        "color": req.color,
        "avatar": req.avatar,
        "system_prompt": req.system_prompt,
    }
    agents[aid] = agent
    _save_agents()
    return agent


@app.put("/api/agents/{agent_id}")
async def update_agent(agent_id: str, req: AgentUpdate):
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    a = agents[agent_id]
    if req.name is not None:
        a["name"] = req.name
    if req.color is not None:
        a["color"] = req.color
    if req.avatar is not None:
        a["avatar"] = req.avatar
    if req.system_prompt is not None:
        a["system_prompt"] = req.system_prompt
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
        "is_streaming": c.get("is_streaming", False),
    } for c in convs]


@app.post("/api/conversations")
async def create_conversation(req: ConversationCreate):
    from datetime import datetime
    cid = uuid.uuid4().hex[:12]
    conv = {
        "id": cid,
        "name": req.name,
        "created_at": datetime.now().isoformat(),
        "messages": [],
        "is_streaming": False,
    }
    conversations[cid] = conv
    _save_conversation(conv)
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
    del conversations[conv_id]
    _delete_conversation_file(conv_id)
    return {"ok": True}


class ConversationUpdate(BaseModel):
    name: Optional[str] = None

@app.put("/api/conversations/{conv_id}")
async def update_conversation(conv_id: str, req: ConversationUpdate):
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")
    conv = conversations[conv_id]
    if req.name is not None:
        conv["name"] = req.name
    _save_conversation(conv)
    return conv


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


def extract_mentioned_agents(text: str) -> list[str]:
    """Extract @mentioned agent IDs from any text."""
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
        if text[idx] == '@':
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


# ── SSE Helpers ───────────────────────────────────────────────────────────────

def _sse_event(event_type: str, data: dict) -> str:
    payload = {"type": event_type, **data}
    return json.dumps(payload, ensure_ascii=False) + "\n"


async def _stream_agent_response(
    agent_id: str, messages: list[dict]
) -> AsyncGenerator[str, None]:
    """Call Hermes API Server and yield SSE events for one agent."""
    headers = {"Content-Type": "application/json"}
    if HERMES_API_KEY:
        headers["Authorization"] = f"Bearer {HERMES_API_KEY}"

    payload = {"model": "hermes-agent", "messages": messages, "stream": True}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{HERMES_API_URL}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=600),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    yield _sse_event("error", {"agent_id": agent_id, "error": f"HTTP {resp.status}: {error_text[:200]}"})
                    return

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
                                    yield _sse_event("text", {"agent_id": agent_id, "text": content})
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
                                yield _sse_event("text", {"agent_id": agent_id, "text": content})
                        except json.JSONDecodeError:
                            pass

    except Exception as e:
        yield _sse_event("error", {"agent_id": agent_id, "error": str(e)[:200]})


# ── Message API (SSE streaming) ───────────────────────────────────────────────

@app.post("/api/conversations/{conv_id}/message")
async def send_message(conv_id: str, req: MessageRequest):
    if conv_id not in conversations:
        raise HTTPException(404, "Conversation not found")

    conv = conversations[conv_id]
    agent_list = list(agents.values())

    if not agent_list:
        raise HTTPException(400, "No agents configured")

    # Parse @mentions
    target_ids, cleaned_text = parse_mentions(req.content, agent_list)

    # If no @mentions, broadcast to all
    if not target_ids:
        target_ids = [a["id"] for a in agent_list]

    # Validate targets
    for tid in target_ids:
        if tid not in agents:
            raise HTTPException(400, f"Unknown agent: {tid}")

    # Store user message
    from datetime import datetime
    user_msg = {
        "role": "user",
        "content": req.content,
        "agent_id": None,
        "timestamp": datetime.now().isoformat(),
    }
    conv["messages"].append(user_msg)
    conv["is_streaming"] = True
    _save_conversation(conv)

    # Agent processing queue: (agent_id, depth)
    MAX_MENTION_DEPTH = 1000
    MAX_RESPONSES_PER_AGENT = 3  # each agent can respond at most 3 times per user message
    agent_queue: list[tuple[str, int]] = [(tid, 0) for tid in target_ids]
    agent_response_count: dict[str, int] = {}

    def _build_context_text() -> str:
        """Build full conversation context as text for the system prompt."""
        lines = []
        for msg in conv["messages"]:
            if msg["role"] == "user":
                lines.append(f"[用户]: {msg['content']}")
            elif msg["role"] == "assistant":
                aname = agents.get(msg.get("agent_id", ""), {}).get("name", "Agent")
                lines.append(f"[{aname}]: {msg['content']}")
        return "\n\n".join(lines) if lines else "(暂无对话历史)"

    async def event_stream():
        while agent_queue:
            agent_id, depth = agent_queue.pop(0)
            if agent_id not in agents:
                continue
            # Per-agent response limit
            if agent_response_count.get(agent_id, 0) >= MAX_RESPONSES_PER_AGENT:
                continue
            agent_response_count[agent_id] = agent_response_count.get(agent_id, 0) + 1

            agent = agents[agent_id]
            yield _sse_event("agent_start", {"agent_id": agent_id, "name": agent["name"]})

            context_text = _build_context_text()
            full_system_prompt = (
                f"{agent['system_prompt']}\n\n"
                f"---\n以下是完整的对话历史（包含所有参与者）：\n\n{context_text}\n---\n\n"
                f"请以 [{agent['name']}] 的身份回复最后一条消息。"
                f"你的回复会自动添加到对话中，不需要加 [{agent['name']}] 前缀。"
            )

            agent_messages = [
                {"role": "system", "content": full_system_prompt},
                {"role": "user", "content": "请回复。"},
            ]

            # Stream response
            full_response = ""
            async for event_str in _stream_agent_response(agent_id, agent_messages):
                yield event_str
                try:
                    evt = json.loads(event_str.strip())
                    if evt.get("type") == "text":
                        full_response += evt.get("text", "")
                except json.JSONDecodeError:
                    pass

            yield _sse_event("agent_done", {"agent_id": agent_id, "full_response": full_response})

            # Store response immediately
            if full_response:
                clean_response = full_response
                prefix = f"[{agent['name']}]:"
                if clean_response.startswith(prefix):
                    clean_response = clean_response[len(prefix):].strip()
                elif clean_response.startswith(f"[{agent['name']}]:"):
                    clean_response = clean_response[len(f"[{agent['name']}]:"):].strip()

                conv["messages"].append({
                    "role": "assistant",
                    "content": clean_response,
                    "agent_id": agent_id,
                    "timestamp": datetime.now().isoformat(),
                })
                _save_conversation(conv)

                # Check for @mentions
                if depth < MAX_MENTION_DEPTH:
                    mentioned = extract_mentioned_agents(full_response)
                    for mid in mentioned:
                        if mid in agents:
                            agent_queue.append((mid, depth + 1))
                            yield _sse_event("mention_trigger", {
                                "from_agent": agent_id,
                                "to_agent": mid,
                                "to_name": agents[mid]["name"],
                            })

        conv["is_streaming"] = False
        _save_conversation(conv)
        yield _sse_event("done", {})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


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
    return {"status": "ok", "hermes_api": hermes_ok, "agents": len(agents), "conversations": len(conversations)}


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
    print(f"Starting Agent Group Chat on http://localhost:{SERVER_PORT}")
    print(f"Hermes API Server: {HERMES_API_URL}")
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT, log_level="info")
