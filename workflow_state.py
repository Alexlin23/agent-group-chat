"""Workflow state definition for LangGraph engine.

The shared state that flows through every node in a workflow graph.
All node functions receive this state and return partial updates.

Split into two TypedDicts:
- WorkflowState: serializable, checkpointable (persisted between steps)
- WorkflowContext: runtime-only, not checkpointed (injected per-run)
"""

import operator
from typing import Annotated, Any, Literal, TypedDict


class WorkflowContext(TypedDict, total=False):
    """Runtime-only context — NOT serialized, NOT checkpointed.

    These are injected at run start and available to all nodes,
    but never written to checkpoints or persistent storage.
    """
    event_buffer: Any         # SSE 事件缓冲
    agents_ref: dict[str, dict]  # agent 定义引用
    hermes_url: str           # Hermes API 地址
    hermes_key: str           # Hermes API Key


class WorkflowState(TypedDict, total=False):
    """Serializable workflow state — flows through LangGraph nodes.

    This is checkpointable and can be persisted between steps.
    All node functions receive this and return partial updates.
    """
    # ── 身份标识 ──
    workflow_id: str          # workflow 定义 ID
    run_id: str               # 本次执行的 run ID

    # ── 图遍历 ──
    current_node: str         # 当前执行到哪个节点
    conditional_result: str   # 条件分支的结果路径名（如 "approved"/"revise"）
    completed_nodes: list[str]  # 已执行完毕的节点 ID（用于 resume 跳过）

    # ── 变量系统 ──
    # 节点间通过变量名传递数据，值可以是任意类型
    variables: dict[str, Any]

    # ── 节点输出 ──
    # 记录每个节点的原始输出，用于分支判断和日志
    node_outputs: dict[str, Any]

    # ── 对话历史 ──
    # 用 Annotated + operator.add 让 LangGraph 自动追加
    messages: Annotated[list[dict], operator.add]

    # ── 执行日志 ──
    # 记录每个节点的执行时间、状态等
    execution_log: Annotated[list[dict], operator.add]

    # ── 运行状态 ──
    status: Literal["running", "paused", "completed", "failed"]
    error: str                # 错误信息（如果失败）

    # ── 人类介入 ──
    human_input: str          # 人类提供的输入
    waiting_for_human: bool   # 是否在等待人类输入
    human_prompt: str         # 向人类展示的问题/提示


# ── 初始状态工厂 ──

def create_initial_state(
    workflow_id: str,
    run_id: str,
    input_text: str,
    ctx: WorkflowContext,
) -> tuple[WorkflowState, WorkflowContext]:
    """Create the initial state and context for a workflow run.

    Returns:
        (state, context) tuple — state is checkpointable, context is runtime-only.
    """
    state = WorkflowState(
        workflow_id=workflow_id,
        run_id=run_id,
        current_node="__start__",
        conditional_result="",
        completed_nodes=[],
        variables={"input": input_text},
        node_outputs={},
        messages=[{"role": "user", "content": input_text}],
        execution_log=[],
        status="running",
        error="",
        human_input="",
        waiting_for_human=False,
        human_prompt="",
    )
    return state, ctx
