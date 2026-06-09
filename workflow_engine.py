"""Workflow engine — execute compiled LangGraph graphs.

This is the main entry point for running workflows.
It takes a workflow definition, builds the graph, and executes it.

Supports:
- Sequential execution
- Conditional branching
- Cycles/loops
- Parallel execution (fan-out/fan-in)
- Human-in-the-loop (pause/resume)
- Checkpointing (MemorySaver)
"""

import uuid
from datetime import datetime
from typing import Any, Optional

from event_buffer import EventBuffer
from workflow_graph import build_graph_from_definition
from workflow_state import WorkflowState, WorkflowContext, create_initial_state


# ── 向后兼容（task_flow.py 仍引用旧名） ──

async def execute_flow(
    flow: dict,
    input_text: str,
    agents: dict,
    hermes_url: str,
    hermes_key: str,
    event_buffer=None,
) -> dict:
    """Backward-compatible wrapper for old TaskFlowManager.

    Returns variables dict on success, raises on failure.
    Note: If the workflow pauses (human-in-the-loop), this returns
    the partial variables — the caller won't know about the pause.
    """
    result = await execute_workflow(
        workflow_def=flow,
        input_text=input_text,
        agents=agents,
        hermes_url=hermes_url,
        hermes_key=hermes_key,
        event_buffer=event_buffer,
    )
    if result.get("status") == "failed":
        raise RuntimeError(result.get("error", "Workflow execution failed"))
    return result.get("variables", {})


# ── Checkpoint 存储 ──
# 运行中的检查点存在内存里，key = run_id
_checkpoints: dict[str, dict] = {}


def save_checkpoint(run_id: str, state: dict):
    """Save a checkpoint for a run (in-memory)."""
    _checkpoints[run_id] = {
        "state": state,
        "saved_at": datetime.now().isoformat(),
    }


def load_checkpoint(run_id: str) -> Optional[dict]:
    """Load a checkpoint for a run."""
    cp = _checkpoints.get(run_id)
    return cp["state"] if cp else None


def clear_checkpoint(run_id: str):
    """Clear a checkpoint after run completes."""
    _checkpoints.pop(run_id, None)


# ── 执行入口 ──

async def execute_workflow(
    workflow_def: dict,
    input_text: str,
    agents: dict[str, dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer: Optional[EventBuffer] = None,
    run_id: Optional[str] = None,
) -> dict:
    """Execute a workflow definition from scratch.

    Args:
        workflow_def: The workflow definition (nodes, edges, etc.).
        input_text: User's input text.
        agents: Agent definitions dict.
        hermes_url: Hermes API URL.
        hermes_key: Hermes API key.
        event_buffer: Optional SSE event buffer.
        run_id: Optional run ID (auto-generated if not provided).

    Returns:
        Final state dict with variables, node_outputs, execution_log, etc.
        If the workflow pauses for human input, status will be "paused"
        and a checkpoint is saved.
    """
    wf_id = workflow_def.get("id", "")
    run_id = run_id or uuid.uuid4().hex[:8]

    if event_buffer:
        event_buffer.push("workflow_start", {
            "workflow_id": wf_id,
            "run_id": run_id,
            "workflow_name": workflow_def.get("name", ""),
            "total_nodes": len(workflow_def.get("nodes", [])),
        })

    # 构建图
    try:
        compiled_graph = build_graph_from_definition(workflow_def)
    except Exception as e:
        if event_buffer:
            event_buffer.push("workflow_error", {"error": f"Failed to build graph: {str(e)[:200]}"})
            event_buffer.push("done", {})
            event_buffer.close()
        return {"status": "failed", "error": f"Graph build failed: {str(e)[:200]}"}

    # 创建初始状态
    ctx = WorkflowContext(
        event_buffer=event_buffer,
        agents_ref=agents,
        hermes_url=hermes_url,
        hermes_key=hermes_key,
    )
    state, _ = create_initial_state(
        workflow_id=wf_id,
        run_id=run_id,
        input_text=input_text,
        ctx=ctx,
    )

    graph_state = {
        **state,
        "event_buffer": event_buffer,
        "agents_ref": agents,
        "hermes_url": hermes_url,
        "hermes_key": hermes_key,
    }

    return await _run_graph(compiled_graph, graph_state, wf_id, run_id, event_buffer)


async def resume_workflow(
    workflow_def: dict,
    run_id: str,
    human_input: str,
    agents: dict[str, dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer: Optional[EventBuffer] = None,
) -> dict:
    """Resume a paused workflow with human input.

    Args:
        workflow_def: The workflow definition.
        run_id: The run ID of the paused workflow.
        human_input: The human's response to the prompt.
        agents: Agent definitions.
        hermes_url: Hermes API URL.
        hermes_key: Hermes API key.
        event_buffer: Optional SSE event buffer.

    Returns:
        Final state dict, or paused state if more human input needed.
    """
    # 加载检查点
    saved_state = load_checkpoint(run_id)
    if not saved_state:
        return {"status": "failed", "error": f"No checkpoint found for run '{run_id}'"}

    # 注入人类输入
    saved_state["human_input"] = human_input
    saved_state["waiting_for_human"] = False
    saved_state["status"] = "running"

    # 重建图
    try:
        compiled_graph = build_graph_from_definition(workflow_def)
    except Exception as e:
        return {"status": "failed", "error": f"Graph build failed: {str(e)[:200]}"}

    # 恢复运行时上下文
    saved_state["event_buffer"] = event_buffer
    saved_state["agents_ref"] = agents
    saved_state["hermes_url"] = hermes_url
    saved_state["hermes_key"] = hermes_key

    wf_id = workflow_def.get("id", "")

    if event_buffer:
        event_buffer.push("workflow_resumed", {
            "workflow_id": wf_id,
            "run_id": run_id,
            "human_input": human_input[:200],
        })

    return await _run_graph(compiled_graph, saved_state, wf_id, run_id, event_buffer)


async def _run_graph(
    compiled_graph: Any,
    graph_state: dict,
    wf_id: str,
    run_id: str,
    event_buffer: Optional[EventBuffer],
) -> dict:
    """Internal: run the compiled graph and handle checkpointing."""
    try:
        result = await compiled_graph.ainvoke(graph_state)
    except Exception as e:
        error_msg = f"Workflow execution failed: {str(e)[:300]}"
        if event_buffer:
            event_buffer.push("workflow_error", {"error": error_msg})
            event_buffer.push("done", {})
            event_buffer.close()
        clear_checkpoint(run_id)
        return {
            "status": "failed",
            "error": error_msg,
            "variables": graph_state.get("variables", {}),
            "node_outputs": graph_state.get("node_outputs", {}),
            "execution_log": graph_state.get("execution_log", []),
        }

    # 检查是否需要暂停等待人类输入
    if result.get("waiting_for_human"):
        # 只保存可序列化的字段（来自 WorkflowState）
        serializable_keys = set(WorkflowState.__annotations__.keys())
        cp_state = {k: v for k, v in result.items() if k in serializable_keys}
        save_checkpoint(run_id, cp_state)
        if event_buffer:
            event_buffer.push("workflow_paused", {"workflow_id": wf_id, "run_id": run_id, "human_prompt": result.get("human_prompt", "")})
        return {
            "status": "paused", "run_id": run_id,
            "human_prompt": result.get("human_prompt", ""),
            "variables": result.get("variables", {}),
            "node_outputs": result.get("node_outputs", {}),
            "execution_log": result.get("execution_log", []),
        }

    # 检查是否失败
    if result.get("status") == "failed":
        clear_checkpoint(run_id)
        if event_buffer:
            event_buffer.push("workflow_error", {"error": result.get("error", "")})
            event_buffer.push("done", {})
            event_buffer.close()
        return {
            "status": "failed",
            "error": result.get("error", ""),
            "variables": result.get("variables", {}),
            "node_outputs": result.get("node_outputs", {}),
            "execution_log": result.get("execution_log", []),
        }

    # 正常完成
    clear_checkpoint(run_id)

    if event_buffer:
        event_buffer.push("workflow_done", {
            "workflow_id": wf_id,
            "run_id": run_id,
            "variables": {
                k: (v[:200] + "..." if isinstance(v, str) and len(v) > 200 else v)
                for k, v in result.get("variables", {}).items()
            },
        })
        event_buffer.push("done", {})
        event_buffer.close()

    return {
        "status": result.get("status", "completed"),
        "error": result.get("error", ""),
        "variables": result.get("variables", {}),
        "node_outputs": result.get("node_outputs", {}),
        "execution_log": result.get("execution_log", []),
        "messages": result.get("messages", []),
    }
