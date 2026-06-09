"""Workflow graph builder — construct LangGraph StateGraph from workflow definition.

Takes a workflow definition (nodes + edges) and builds a compiled
LangGraph StateGraph that can be invoked with (state, context).

Supports:
- Sequential execution (edges)
- Conditional branching (conditional_edges)
- Cycles/loops (edges back to earlier nodes)
- Future: parallel (Send API), human-in-the-loop (interrupt)
"""

import operator
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, StateGraph

from workflow_nodes import agent_node, condition_node, parallel_node, human_node
from workflow_state import WorkflowState, WorkflowContext


# ── Graph State (extends WorkflowState with context for LangGraph) ──

class GraphState(TypedDict, total=False):
    """Full state for the compiled graph — includes both serializable
    state and runtime context so LangGraph can pass everything through."""
    # Serializable fields (from WorkflowState)
    workflow_id: str
    run_id: str
    current_node: str
    conditional_result: str
    completed_nodes: list[str]  # 已完成节点，resume 时跳过
    variables: dict[str, Any]
    node_outputs: dict[str, Any]
    messages: Annotated[list[dict], operator.add]
    execution_log: Annotated[list[dict], operator.add]
    status: str
    error: str
    human_input: str
    waiting_for_human: bool
    human_prompt: str
    # Runtime context (not checkpointed)
    event_buffer: Any
    agents_ref: dict[str, dict]
    hermes_url: str
    hermes_key: str


# ── Node Factory ──

def _make_node_func(node_def: dict):
    """Create a LangGraph node function from a node definition.

    Supports node types:
    - "agent" (default): standard agent call
    - "parallel": fan-out/fan-in over a list
    - "human": pause for human input

    Returns an async function that receives GraphState and returns partial update.
    """
    node_id = node_def["id"]
    agent_id = node_def.get("agent_id", "")
    prompt = node_def.get("prompt", "")
    output_var = node_def.get("output_var", "")
    node_name = node_def.get("name", node_id)
    node_type = node_def.get("type", "agent")

    async def _node_func(state: GraphState) -> dict:
        # 恢复时跳过已完成的节点
        completed = state.get("completed_nodes", [])
        if node_id in completed:
            return {}

        ctx = WorkflowContext(
            event_buffer=state.get("event_buffer"),
            agents_ref=state.get("agents_ref", {}),
            hermes_url=state.get("hermes_url", ""),
            hermes_key=state.get("hermes_key", ""),
        )

        if node_type == "parallel":
            return await parallel_node(
                state=state, ctx=ctx, node_id=node_id,
                agent_id=agent_id, prompt_template=prompt,
                items_var=node_def.get("items_var", "items"),
                item_var=node_def.get("item_var", "item"),
                output_var=output_var, node_name=node_name,
            )
        elif node_type == "human":
            return await human_node(
                state=state, ctx=ctx, node_id=node_id,
                prompt=prompt, output_var=output_var,
                node_name=node_name,
            )
        else:
            return await agent_node(
                state=state, ctx=ctx, node_id=node_id,
                agent_id=agent_id, prompt_template=prompt,
                output_var=output_var, node_name=node_name,
            )

    _node_func.__name__ = f"node_{node_id}"
    return _node_func


def _make_condition_func(cond_def: dict):
    """Create a LangGraph node function for a conditional edge evaluation."""
    node_id = cond_def["from"]
    condition = cond_def.get("condition", "")
    paths = list(cond_def.get("paths", {}).keys())

    async def _cond_func(state: GraphState) -> dict:
        ctx = WorkflowContext(
            event_buffer=state.get("event_buffer"),
            agents_ref=state.get("agents_ref", {}),
            hermes_url=state.get("hermes_url", ""),
            hermes_key=state.get("hermes_key", ""),
        )
        return await condition_node(
            state=state,
            ctx=ctx,
            node_id=f"cond_{node_id}",
            condition_prompt=condition,
            paths=paths,
        )

    _cond_func.__name__ = f"condition_{node_id}"
    return _cond_func


# ── Graph Builder ──

def build_graph_from_definition(workflow_def: dict) -> Any:
    """Build and compile a LangGraph StateGraph from a workflow definition.

    Args:
        workflow_def: Workflow definition dict with 'nodes', 'edges', 'conditional_edges'.

    Returns:
        Compiled LangGraph graph (can be invoked with graph.ainvoke(state)).
    """
    nodes = workflow_def.get("nodes", [])
    edges = workflow_def.get("edges", [])
    cond_edges = workflow_def.get("conditional_edges", [])

    # 构建 node_map 和 edge_map
    node_map = {n["id"]: n for n in nodes}
    edge_map: dict[str, list[str]] = {}
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        edge_map.setdefault(src, []).append(dst)

    # 收集有条件边的节点
    cond_sources = {ce["from"] for ce in cond_edges}

    # 创建 StateGraph
    graph = StateGraph(GraphState)

    # 添加普通节点
    for node_def in nodes:
        node_id = node_def["id"]
        func = _make_node_func(node_def)
        graph.add_node(node_id, func)

    # 添加条件评估节点（每个条件边对应一个评估节点）
    for ce in cond_edges:
        cond_node_id = f"cond_{ce['from']}"
        func = _make_condition_func(ce)
        graph.add_node(cond_node_id, func)

    # 设置入口
    start_targets = edge_map.get("__start__", [])
    if not start_targets:
        raise ValueError("No edge from __start__")
    graph.set_entry_point(start_targets[0])

    # 添加普通边
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src == "__start__":
            continue  # entry point already set
        if src in cond_sources:
            continue  # handled by conditional edges
        if dst == "__end__":
            graph.add_edge(src, END)
        else:
            graph.add_edge(src, dst)

    # 条件边：先走到评估节点，再根据结果路由
    for ce in cond_edges:
        src = ce["from"]
        cond_node_id = f"cond_{src}"
        paths = ce.get("paths", {})

        # 从源节点到条件评估节点
        # 如果源节点也是普通节点，需要在普通边处理后加条件边
        # 这里用条件边：从源节点 → 条件评估节点
        # 然后从条件评估节点 → 根据结果路由

        # 先删掉源节点到 __end__ 的普通边（如果有的话）
        # 改用条件边替代

        # 添加条件评估节点的路由
        path_map = {}
        for path_name, target in paths.items():
            if target == "__end__":
                path_map[path_name] = END
            else:
                path_map[path_name] = target

        # 从源节点到条件评估节点
        graph.add_edge(src, cond_node_id)

        # 从条件评估节点根据结果路由
        graph.add_conditional_edges(
            cond_node_id,
            lambda state, _paths=path_map: _paths.get(
                state.get("conditional_result", ""),
                END,
            ),
        )

    return graph.compile()
