"""Workflow node functions for LangGraph engine.

Each node type is a function that receives (state, context) and returns
a partial WorkflowState update. LangGraph merges the update into state.

Node types:
- agent_node: Call an agent via Hermes API, get response
- condition_node: Use LLM to decide which branch to take
- parallel_node: Fan-out multiple agent calls, fan-in results
- human_node: Pause for human input
"""

import asyncio
import json
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
        "completed_nodes": [node_id],
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
        "completed_nodes": [node_id],
        "conditional_result": matched,
        "execution_log": [{
            "node_id": node_id,
            "status": "condition_resolved",
            "chosen_path": matched,
            "timestamp": datetime.now().isoformat(),
        }],
    }


# ── 并行节点 ──

async def parallel_node(
    state: WorkflowState,
    ctx: WorkflowContext,
    node_id: str,
    agent_id: str,
    prompt_template: str,
    items_var: str,
    item_var: str,
    output_var: str,
    node_name: str = "",
) -> dict:
    """Execute an agent node in parallel for each item in a list.

    Fan-out: run the same agent+prompt for each item in items_var.
    Fan-in: collect all results into output_var as a JSON list.

    Args:
        state: Current workflow state.
        ctx: Runtime context.
        node_id: The node's ID.
        agent_id: Which agent to call.
        prompt_template: Prompt template. Can use {{item_var}} for each item.
        items_var: Variable name containing a list (JSON array or newline-separated).
        item_var: Variable name for the current item in the template.
        output_var: Variable name to store the merged results.
        node_name: Display name.

    Returns:
        Partial WorkflowState update with merged results.
    """
    event_buffer = ctx.get("event_buffer")
    display_name = node_name or node_id

    # 获取要并行处理的 items
    raw_items = state.get("variables", {}).get(items_var, "")
    if isinstance(raw_items, list):
        items = raw_items
    elif isinstance(raw_items, str):
        # 尝试 JSON 解析，失败则按换行分割
        try:
            items = json.loads(raw_items)
            if not isinstance(items, list):
                items = [raw_items]
        except (json.JSONDecodeError, ValueError):
            items = [line.strip() for line in raw_items.split("\n") if line.strip()]
    else:
        items = [raw_items]

    if not items:
        return {
            "current_node": node_id,
            "variables": {**state.get("variables", {}), output_var: "[]"},
            "execution_log": [{
                "node_id": node_id,
                "status": "skipped",
                "reason": "empty items list",
                "timestamp": datetime.now().isoformat(),
            }],
        }

    if event_buffer:
        event_buffer.push("parallel_start", {
            "node_id": node_id,
            "node_name": display_name,
            "item_count": len(items),
        })

    # 为每个 item 构建子状态并并行执行
    async def _run_one(idx: int, item: Any) -> dict:
        # 把 item 注入到变量中
        item_vars = dict(state.get("variables", {}))
        item_vars[item_var] = str(item)

        sub_state = {**state, "variables": item_vars}
        sub_node_id = f"{node_id}[{idx}]"

        return await agent_node(
            state=sub_state,
            ctx=ctx,
            node_id=sub_node_id,
            agent_id=agent_id,
            prompt_template=prompt_template,
            output_var="",  # 不写入子状态的变量
            node_name=f"{display_name}[{idx}]",
        )

    # 并行执行
    results = await asyncio.gather(
        *[_run_one(i, item) for i, item in enumerate(items)],
        return_exceptions=True,
    )

    # 收集结果
    outputs = []
    all_logs = []
    all_messages = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            outputs.append(f"ERROR: {str(result)[:200]}")
            all_logs.append({
                "node_id": f"{node_id}[{i}]",
                "status": "failed",
                "error": str(result)[:200],
                "timestamp": datetime.now().isoformat(),
            })
        elif isinstance(result, dict):
            if result.get("status") == "failed":
                outputs.append(f"ERROR: {result.get('error', 'unknown')}")
            else:
                # 从 node_outputs 中取结果
                node_out = result.get("node_outputs", {})
                outputs.append(node_out.get(f"{node_id}[{i}]", result.get("error", "")))
            all_logs.extend(result.get("execution_log", []))
            all_messages.extend(result.get("messages", []))

    if event_buffer:
        event_buffer.push("parallel_done", {
            "node_id": node_id,
            "node_name": display_name,
            "completed": len([o for o in outputs if not str(o).startswith("ERROR:")]),
            "failed": len([o for o in outputs if str(o).startswith("ERROR:")]),
        })

    # 合并结果到变量
    variables = dict(state.get("variables", {}))
    variables[output_var] = json.dumps(outputs, ensure_ascii=False)
    node_outputs = dict(state.get("node_outputs", {}))
    node_outputs[node_id] = outputs

    return {
        "current_node": node_id,
        "completed_nodes": [node_id],
        "variables": variables,
        "node_outputs": node_outputs,
        "messages": all_messages,
        "execution_log": all_logs,
    }


# ── 人类介入节点 ──

async def human_node(
    state: WorkflowState,
    ctx: WorkflowContext,
    node_id: str,
    prompt: str,
    output_var: str = "human_response",
    node_name: str = "",
) -> dict:
    """Pause workflow and wait for human input.

    In the current implementation, this pushes an event and sets
    waiting_for_human=True. The engine should check this field and
    pause execution until human_input is provided.

    Args:
        state: Current workflow state.
        ctx: Runtime context.
        node_id: The node's ID.
        prompt: What to ask the human.
        output_var: Variable name to store the human's response.
        node_name: Display name.

    Returns:
        Partial WorkflowState update with waiting_for_human=True.
    """
    event_buffer = ctx.get("event_buffer")
    display_name = node_name or node_id

    rendered = render_template(prompt, state.get("variables", {}))

    if event_buffer:
        event_buffer.push("human_input_required", {
            "node_id": node_id,
            "node_name": display_name,
            "prompt": rendered,
        })

    # 如果已经有 human_input（从恢复执行传入），使用它
    human_input = state.get("human_input", "")
    if human_input and human_input.strip():
        variables = dict(state.get("variables", {}))
        variables[output_var] = human_input
        if event_buffer:
            event_buffer.push("human_input_received", {
                "node_id": node_id,
                "input": human_input[:200],
            })
        return {
            "current_node": node_id,
            "completed_nodes": [node_id],
            "variables": variables,
            "waiting_for_human": False,
            "human_input": "",
            "execution_log": [{
                "node_id": node_id,
                "status": "human_input_received",
                "timestamp": datetime.now().isoformat(),
            }],
        }

    # 没有输入，暂停
    return {
        "current_node": node_id,
        "status": "paused",
        "waiting_for_human": True,
        "human_prompt": rendered,
        "execution_log": [{
            "node_id": node_id,
            "status": "waiting_for_human",
            "timestamp": datetime.now().isoformat(),
        }],
    }
