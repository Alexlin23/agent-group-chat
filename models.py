"""Pydantic models — shared request/response schemas."""

from typing import Optional
from pydantic import BaseModel


# ── Agent ─────────────────────────────────────────────────────────────────────

class AgentCreate(BaseModel):
    id: Optional[str] = None
    name: str
    color: str = "#888888"
    avatar: str = "🤖"
    description: str = ""
    system_prompt: str = ""


class AgentUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    avatar: Optional[str] = None
    description: Optional[str] = None
    system_prompt: Optional[str] = None


# ── Conversation ──────────────────────────────────────────────────────────────

class ConversationCreate(BaseModel):
    name: str = "新对话"


class ConversationUpdate(BaseModel):
    name: Optional[str] = None
    default_agent_id: Optional[str] = None


class MessageRequest(BaseModel):
    content: str
    targets: list[str] = []


# ── Task Flow ─────────────────────────────────────────────────────────────────

class FlowStepCreate(BaseModel):
    id: Optional[str] = None
    agent_id: str
    name: str
    prompt_template: str
    input_vars: list[str] = []
    output_var: str = ""
    timeout: int = 300


class TaskFlowCreate(BaseModel):
    name: str
    description: str = ""
    steps: list[FlowStepCreate] = []


class TaskFlowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    steps: Optional[list[FlowStepCreate]] = None


class TaskFlowGenerateRequest(BaseModel):
    description: str


class TaskFlowRunRequest(BaseModel):
    input_text: str
