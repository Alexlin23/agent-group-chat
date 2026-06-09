"""Agent CRUD API routes."""

import uuid
from fastapi import APIRouter, HTTPException

from models import AgentCreate, AgentUpdate
from storage import save_agents

router = APIRouter(prefix="/api/agents", tags=["agents"])


def _get_agents():
    import app_state
    return app_state.agents


def _get_agents_file():
    import sys
    return sys.modules['__main__'].AGENTS_FILE


@router.get("")
async def list_agents():
    return list(_get_agents().values())


@router.get("/{agent_id}")
async def get_agent(agent_id: str):
    agents = _get_agents()
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    return agents[agent_id]


@router.post("")
async def create_agent(req: AgentCreate):
    agents = _get_agents()
    aid = req.id or uuid.uuid4().hex[:8]
    if aid in agents:
        raise HTTPException(409, "Agent ID already exists")
    agent = {
        "id": aid, "name": req.name, "color": req.color,
        "avatar": req.avatar, "description": req.description,
        "system_prompt": req.system_prompt,
    }
    agents[aid] = agent
    save_agents(_get_agents_file(), agents)
    return agent


@router.put("/{agent_id}")
async def update_agent(agent_id: str, req: AgentUpdate):
    agents = _get_agents()
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    a = agents[agent_id]
    if req.name is not None: a["name"] = req.name
    if req.color is not None: a["color"] = req.color
    if req.avatar is not None: a["avatar"] = req.avatar
    if req.description is not None: a["description"] = req.description
    if req.system_prompt is not None: a["system_prompt"] = req.system_prompt
    save_agents(_get_agents_file(), agents)
    return a


@router.delete("/{agent_id}")
async def delete_agent(agent_id: str):
    agents = _get_agents()
    if agent_id not in agents:
        raise HTTPException(404, "Agent not found")
    del agents[agent_id]
    save_agents(_get_agents_file(), agents)
    # Clean up dangling default_agent_id references in conversations
    import app_state
    from storage import save_conversation as _save_conv
    import sys
    data_dir = sys.modules['__main__'].DATA_DIR
    for conv in app_state.conversations.values():
        if conv.get("default_agent_id") == agent_id:
            conv["default_agent_id"] = None
            _save_conv(data_dir, conv)
    return {"ok": True}
