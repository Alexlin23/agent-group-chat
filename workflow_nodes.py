"""Workflow node functions for LangGraph engine.

Each node type is a function that receives (state, context) and returns
a partial WorkflowState update. LangGraph merges the update into state.

Node types:
- agent_node: Call an agent via Hermes API, get response
- condition_node: Use LLM to decide which branch to take
- human_node: Pause for human input
- merge_node: Combine outputs from parallel branches (future)
"""

import re
from datetime import datetime
from typing import Any

from hermes_client import stream_hermes
from text_utils import build_context_text, strip_agent_prefix
from workflow_state import WorkflowState, WorkflowContext


# ── Skill 缓存 ──

_SKILL_CACHE = None


def _load_project_skill() -> str:
    """Load the project skill file (cached)."""
    global _SKILL_CACHE
    if _SKILL_CACHE is not None:
        return _SKILL_CACHE
    from pathlib import Path
    skill_path = Path(__file__).parent / ".hermes" / "skills" / "agent-group-chat.md"
    if skill_path.exists():
        _SKILL_CACHE = skill_path.read_text(encoding="utf-8") + "\n\n"
    else:
        _SKILL_CACHE = ""
    return _SKILL_CACHE


# ── 模板渲染 ──

def render_template(template: str, variables: dict[str, Any]) -> str:
    """Render a {{var}} template with variables.

    Supports {{var}} for simple substitution.
    """
    def replacer(match):
        var_name = match.group(1).strip()
        val = variables.get(var_name, match.group(0))
        return str(val) if not isinstance(val, str) else val
    return re.sub(r'\{\{(.+?)\}\}', replacer, template)


# ── Agent 节点 ──

async def agent_node(
    state: WorkflowState,
    ctx: WorkflowContext,
    node_id: str,
    agent_id: str,
    prompt_template: str,
    output_var: str = "",
    node_name: str = "",
) -> dict:
    """Execute an agent node — call Hermes API and return the response.

    Args:
        state: Current workflow state.
        ctx: Runtime context (agents, hermes config, event_buffer).
        node_id: The node's ID in the workflow graph.
        agent_id: Which agent to call.
        prompt_template: The prompt template with {{var}} placeholders.
        output_var: Variable name to store the output in.
        node_name: Display name for this node.

    Returns:
        Partial WorkflowState update.
    """
    agents = ctx["agents_ref"]
    event_buffer = ctx.get("event_buffer")
    agent = agents.get(agent_id)

    if not agent:
        return {
            "status": "failed",
            "error": f"Agent '{agent_id}' not found for node '{node_id}'",
        }

    # 渲染模板
    rendered_prompt = render_template(prompt_template, state.get("variables", {}))
    display_name = node_name or node_id

    # 推送事件
    if event_buffer:
        event_buffer.push("node_start", {
            "node_id": node_id,
            "node_name": display_name,
            "agent_id": agent_id,
            "agent_name": agent["name"],
        })

    # 构建 system prompt
    context_text = build_context_text(state.get("messages", []), agents)
    skill_text = _load_project_skill()
    system_prompt = (
        f"{agent.get('system_prompt', '')}\n\n"
        f"{skill_text}"
        f"---\n工作流节点: {display_name}\n---\n\n"
        f"{rendered_prompt}"
    )

    # 流式调用
    full_response = ""
    try:
        async for chunk in stream_hermes(
            ctx["hermes_url"], ctx["hermes_key"], system_prompt,
        ):
            full_response += chunk
            if event_buffer:
                event_buffer.push("node_text", {
                    "node_id": node_id,
                    "agent_id": agent_id,
                    "text": chunk,
                })
    except RuntimeError as e:
        if event_buffer:
            event_buffer.push("node_error", {
                "node_id": node_id,
                "agent_id": agent_id,
                "error": str(e),
            })
        return {
            "status": "failed",
            "error": f"Node '{node_id}' failed: {str(e)[:200]}",
        }

    # 清理响应
    clean_response = strip_agent_prefix(full_response, agent["name"])

    # 推送完成事件
    if event_buffer:
        event_buffer.push("node_done", {
            "node_id": node_id,
            "node_name": display_name,
            "agent_id": agent_id,
            "agent_name": agent["name"],
            "output_var": output_var,
            "full_response": clean_response,
        })

    # 构建状态更新
    variables = dict(state.get("variables", {}))
    node_outputs = dict(state.get("node_outputs", {}))
    if output_var:
        variables[output_var] = clean_response
    node_outputs[node_id] = clean_response

    new_message = {
        "role": "assistant",
        "content": clean_response,
        "agent_id": agent_id,
        "node_id": node_id,
        "timestamp": datetime.now().isoformat(),
    }

    log_entry = {
        "node_id": node_id,
        "agent_id": agent_id,
        "status": "completed",
        "timestamp": datetime.now().isoformat(),
    }

    return {
        "current_node": node_id,
        "variables": variables,
        "node_outputs": node_outputs,
        "messages": [new_message],
        "execution_log": [log_entry],
    }


# ── 条件节点 ──

async def condition_node(
    state: WorkflowState,
    ctx: WorkflowContext,
    node_id: str,
    condition_prompt: str,
    paths: list[str],
    node_name: str = "",
) -> dict:
    """Execute a condition node — ask LLM which branch to take.

    Args:
        state: Current workflow state.
        ctx: Runtime context.
        node_id: The node's ID.
        condition_prompt: Prompt that asks the LLM to evaluate a condition.
        paths: List of possible path names (e.g., ["approved", "revise"]).
        node_name: Display name.

    Returns:
        Partial WorkflowState update with conditional_result set.
    """
    event_buffer = ctx.get("event_buffer")
    display_name = node_name or node_id

    if event_buffer:
        event_buffer.push("condition_start", {
            "node_id": node_id,
            "node_name": display_name,
            "paths": paths,
        })

    # 渲染条件提示
    rendered = render_template(condition_prompt, state.get("variables", {}))

    # 让 LLM 判断走哪条路
    system_prompt = (
        f"你是一个工作流条件判断器。根据以下信息决定走哪条路径。\n\n"
        f"可用路径：{', '.join(paths)}\n\n"
        f"你必须只输出一个路径名，不要输出其他内容。\n\n"
        f"---\n{rendered}"
    )

    messages = [{"role": "system", "content": system_prompt}]

    from hermes_client import call_hermes
    try:
        response = await call_hermes(
            ctx["hermes_url"], ctx["hermes_key"], messages,
            temperature=0.1, max_tokens=50,
        )
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Condition node '{node_id}' failed: {str(e)[:200]}",
        }

    # 从响应中提取路径名
    chosen = response.strip().lower()
    matched = None
    for path in paths:
        if path.lower() in chosen:
            matched = path
            break

    if not matched:
        # 默认走第一个路径
        matched = paths[0]
        if event_buffer:
            event_buffer.push("condition_fallback", {
                "node_id": node_id,
                "raw_response": response[:100],
                "fallback_to": matched,
            })

    if event_buffer:
        event_buffer.push("condition_done", {
            "node_id": node_id,
            "node_name": display_name,
            "chosen_path": matched,
        })

    return {
        "current_node": node_id,
        "conditional_result": matched,
        "execution_log": [{
            "node_id": node_id,
            "status": "condition_resolved",
            "chosen_path": matched,
            "timestamp": datetime.now().isoformat(),
        }],
    }
