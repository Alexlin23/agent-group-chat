"""Conversation + Message API routes."""

import asyncio
import re
import uuid
from datetime import datetime
from typing import Optional

import app_state
from fastapi import APIRouter, Depends, HTTPException, Path as FPath, Query
from fastapi.responses import StreamingResponse

from event_buffer import EventBuffer, format_sse
from message_bus import MessageBus, MessageBusManager
from models import ConversationCreate, ConversationUpdate, MessageRequest
from orchestrator import Orchestrator
from storage import save_conversation, delete_conversation_file
from text_utils import parse_mentions

router = APIRouter(prefix="/api/conversations", tags=["conversations"])


# ── Path Validation ───────────────────────────────────────────────────────────

def validate_conv_id(conv_id: str = FPath(...)) -> str:
    if not re.fullmatch(r'[0-9a-f]{12}', conv_id):
        raise HTTPException(400, "Invalid conversation ID")
    return conv_id


# ── State accessors (avoid circular imports) ──────────────────────────────────

def _state():
    import sys
    return sys.modules['__main__']


@router.get("")
async def list_conversations():
    s = _state()
    convs = sorted(app_state.conversations.values(), key=lambda c: c.get("created_at", ""), reverse=True)
    return [{
        "id": c["id"],
        "name": c["name"],
        "created_at": c.get("created_at", ""),
        "message_count": len(c.get("messages", [])),
        "default_agent_id": c.get("default_agent_id"),
        "is_streaming": c["id"] in s.active_tasks and not s.active_tasks[c["id"]].done(),
    } for c in convs]


@router.post("")
async def create_conversation(req: ConversationCreate):
    s = _state()
    cid = uuid.uuid4().hex[:12]
    conv = {
        "id": cid, "name": req.name,
        "created_at": datetime.now().isoformat(),
        "messages": [],
    }
    app_state.conversations[cid] = conv
    save_conversation(s.DATA_DIR, conv)
    await s.bus_manager.get_or_create(cid)
    return conv


@router.get("/{conv_id}")
async def get_conversation(conv_id: str = Depends(validate_conv_id)):
    s = _state()
    if conv_id not in app_state.conversations:
        raise HTTPException(404, "Conversation not found")
    return app_state.conversations[conv_id]


@router.delete("/{conv_id}")
async def delete_conversation(conv_id: str = Depends(validate_conv_id)):
    s = _state()
    if conv_id not in app_state.conversations:
        raise HTTPException(404, "Conversation not found")
    if conv_id in s.active_tasks and not s.active_tasks[conv_id].done():
        s.active_tasks[conv_id].cancel()
    s.active_tasks.pop(conv_id, None)
    s.active_buffers.pop(conv_id, None)
    s.bus_manager.remove(conv_id)
    del app_state.conversations[conv_id]
    delete_conversation_file(s.DATA_DIR, conv_id)
    return {"ok": True}


@router.put("/{conv_id}")
async def update_conversation(req: ConversationUpdate, conv_id: str = Depends(validate_conv_id)):
    s = _state()
    if conv_id not in app_state.conversations:
        raise HTTPException(404, "Conversation not found")
    conv = app_state.conversations[conv_id]
    if req.name is not None:
        conv["name"] = req.name
    if req.default_agent_id is not None:
        conv["default_agent_id"] = req.default_agent_id or None
    save_conversation(s.DATA_DIR, conv)
    return conv


@router.post("/{conv_id}/message")
async def send_message(req: MessageRequest, conv_id: str = Depends(validate_conv_id)):
    """Start agent processing for a message. Returns immediately."""
    s = _state()
    if conv_id not in app_state.conversations:
        raise HTTPException(404, "Conversation not found")

    agent_list = list(app_state.agents.values())
    if not agent_list:
        raise HTTPException(400, "No agents configured")

    # Parse @mentions
    target_ids, cleaned_text = parse_mentions(req.content, agent_list)
    if not target_ids:
        # Use per-conversation default agent, fallback to first agent
        conv = app_state.conversations[conv_id]
        default_id = conv.get("default_agent_id")
        if default_id and default_id in app_state.agents:
            target_ids = [default_id]
        else:
            target_ids = [agent_list[0]["id"]]

    for tid in target_ids:
        if tid not in app_state.agents:
            raise HTTPException(400, f"Unknown agent: {tid}")

    # Store user message via MessageBus
    bus = await s.bus_manager.get_or_create(conv_id, app_state.conversations[conv_id].get("messages", []))
    user_msg = {"role": "user", "content": req.content, "agent_id": None}
    await bus.append(user_msg)
    app_state.conversations[conv_id]["messages"] = bus.messages
    save_conversation(s.DATA_DIR, app_state.conversations[conv_id])

    # Cut old @mention chain
    s.orchestrator.cut_mention_chain(conv_id)

    # Create EventBuffer for this task
    buffer = EventBuffer()
    s.active_buffers[conv_id] = buffer

    async def _run(my_buffer=buffer):
        try:
            await s.orchestrator.process_message(
                conv_id=conv_id,
                user_message=req.content,
                target_ids=target_ids,
                bus=bus,
                event_buffer=my_buffer,
            )
            app_state.conversations[conv_id]["messages"] = bus.messages
            save_conversation(s.DATA_DIR, app_state.conversations[conv_id])
        except Exception as e:
            my_buffer.push("error", {"error": str(e)[:300]})
            my_buffer.close()
        finally:
            if s.active_buffers.get(conv_id) is my_buffer:
                s.active_buffers.pop(conv_id, None)

    task = asyncio.create_task(_run())
    s.active_tasks[conv_id] = task

    return {"status": "accepted", "conv_id": conv_id, "targets": target_ids}


@router.get("/{conv_id}/stream")
async def stream_conversation(
    conv_id: str = Depends(validate_conv_id),
    last_id: int = Query(-1, alias="last_id"),
):
    """SSE endpoint: stream events for a conversation."""
    s = _state()
    if conv_id not in app_state.conversations:
        raise HTTPException(404, "Conversation not found")

    buffer = s.active_buffers.get(conv_id)

    async def event_generator():
        if buffer is None or not buffer.is_active:
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


@router.get("/{conv_id}/task_status")
async def task_status(conv_id: str = Depends(validate_conv_id)):
    """Check if a task is running for this conversation."""
    s = _state()
    task = s.active_tasks.get(conv_id)
    if task is None:
        return {"status": "idle"}
    if task.done():
        return {"status": "done"}
    return {"status": "running"}
