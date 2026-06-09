"""Workflow engine — execute compiled LangGraph graphs.

This is the main entry point for running workflows.
It takes a workflow definition, builds the graph, and executes it.

Supports:
- Sequential execution
- Conditional branching
- Cycles/loops
- Future: parallel, human-in-the-loop, checkpoints
"""

import uuid
from datetime import datetime
from typing import Any, Optional

from event_buffer import EventBuffer
from workflow_graph import build_graph_from_definition
from workflow_state import WorkflowState, WorkflowContext, create_initial_state


async def execute_workflow(
    workflow_def: dict,
    input_text: str,
    agents: dict[str, dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer: Optional[EventBuffer] = None,
    run_id: Optional[str] = None,
) -> dict:
    """Execute a workflow definition.

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
    """
    wf_id = workflow_def.get("id", "")
    run_id = run_id or uuid.uuid4().hex[:8]

    # 推送工作流开始事件
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
            event_buffer.push("workflow_error", {
                "error": f"Failed to build graph: {str(e)[:200]}",
            })
            event_buffer.push("done", {})
            event_buffer.close()
        return {
            "status": "failed",
            "error": f"Graph build failed: {str(e)[:200]}",
        }

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

    # 扩展状态以包含图需要的运行时字段
    graph_state = {
        **state,
        "event_buffer": event_buffer,
        "agents_ref": agents,
        "hermes_url": hermes_url,
        "hermes_key": hermes_key,
    }

    # 执行
    try:
        result = await compiled_graph.ainvoke(graph_state)
    except Exception as e:
        error_msg = f"Workflow execution failed: {str(e)[:300]}"
        if event_buffer:
            event_buffer.push("workflow_error", {"error": error_msg})
            event_buffer.push("done", {})
            event_buffer.close()
        return {
            "status": "failed",
            "error": error_msg,
            "variables": graph_state.get("variables", {}),
            "node_outputs": graph_state.get("node_outputs", {}),
            "execution_log": graph_state.get("execution_log", []),
        }

    # 推送完成事件
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
