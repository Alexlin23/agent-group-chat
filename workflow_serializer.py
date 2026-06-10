"""Workflow serializer — validate, serialize, and deserialize workflow definitions.

Workflow definition format (stored as JSON):
{
    "id": "wf_abc123",
    "name": "PPT翻译",
    "description": "...",
    "created_at": "2026-06-09T...",
    "updated_at": "2026-06-09T...",
    "nodes": [
        {"id": "analyze", "agent_id": "architect", "name": "分析", "prompt": "...", "output_var": "structure"},
        {"id": "translate", "agent_id": "coder", "name": "翻译", "prompt": "...", "output_var": "translation"}
    ],
    "edges": [
        {"from": "__start__", "to": "analyze"},
        {"from": "analyze", "to": "translate"},
        {"from": "translate", "to": "__end__"}
    ],
    "conditional_edges": [
        {"from": "review", "condition": "...", "paths": {"approved": "__end__", "revise": "translate"}}
    ]
}
"""

from datetime import datetime
from typing import Optional
import uuid


# ── 默认值 ──

NODE_DEFAULTS = {
    "name": "",
    "prompt": "",
    "output_var": "",
    "timeout": 300,
    "code": "",
}


# ── 验证 ──

class ValidationError(Exception):
    """Raised when a workflow definition is invalid."""
    pass


def validate_workflow(data: dict, agents: Optional[dict] = None) -> list[str]:
    """Validate a workflow definition. Returns list of errors (empty = valid).

    Args:
        data: The workflow definition dict.
        agents: Optional agent definitions to validate agent_id references.

    Returns:
        List of error strings. Empty list means valid.
    """
    errors = []

    # 必填字段
    if "name" not in data or not data["name"].strip():
        errors.append("Missing required field: 'name'")

    # 节点验证
    nodes = data.get("nodes", [])
    if not nodes:
        errors.append("Workflow must have at least one node")
    node_ids = set()
    for i, node in enumerate(nodes):
        if "id" not in node:
            errors.append(f"Node {i}: missing 'id'")
        else:
            if node["id"] in node_ids:
                errors.append(f"Node {i}: duplicate id '{node['id']}'")
            node_ids.add(node["id"])
        # human 和 code 类型不需要 agent_id
        node_type = node.get("type", "agent")
        if node_type not in ("human", "code"):
            if "agent_id" not in node:
                errors.append(f"Node '{node.get('id', i)}': missing 'agent_id'")
            elif agents and node["agent_id"] not in agents:
                errors.append(f"Node '{node.get('id', i)}': agent '{node['agent_id']}' not found")
        # parallel 类型需要 items_var 和 item_var
        if node_type == "parallel":
            if "items_var" not in node:
                errors.append(f"Node '{node.get('id', i)}': parallel type requires 'items_var'")
            if "item_var" not in node:
                errors.append(f"Node '{node.get('id', i)}': parallel type requires 'item_var'")
        # code 类型需要非空 code 字段
        if node_type == "code":
            if "code" not in node or not node.get("code", "").strip():
                errors.append(f"Node '{node.get('id', i)}': code type requires non-empty 'code' field")

    # 边验证
    edges = data.get("edges", [])
    special = {"__start__", "__end__"}
    for i, edge in enumerate(edges):
        if "from" not in edge:
            errors.append(f"Edge {i}: missing 'from'")
        elif edge["from"] not in node_ids | special:
            errors.append(f"Edge {i}: 'from' references unknown node '{edge['from']}'")
        if "to" not in edge:
            errors.append(f"Edge {i}: missing 'to'")
        elif edge["to"] not in node_ids | special:
            errors.append(f"Edge {i}: 'to' references unknown node '{edge['to']}'")

    # 条件边验证
    for i, ce in enumerate(data.get("conditional_edges", [])):
        if "from" not in ce:
            errors.append(f"Conditional edge {i}: missing 'from'")
        elif ce["from"] not in node_ids:
            errors.append(f"Conditional edge {i}: 'from' references unknown node '{ce['from']}'")
        if "condition" not in ce or not ce["condition"].strip():
            errors.append(f"Conditional edge {i}: missing or empty 'condition'")
        if "paths" not in ce:
            errors.append(f"Conditional edge {i}: missing 'paths'")
        elif not ce["paths"]:
            errors.append(f"Conditional edge {i}: 'paths' must not be empty")
        else:
            for path_name, target in ce["paths"].items():
                if target not in node_ids | special:
                    errors.append(f"Conditional edge {i}: path '{path_name}' references unknown node '{target}'")

    # 结构验证
    edges = data.get("edges", [])
    start_edges = [e for e in edges if e.get("from") == "__start__"]
    if len(start_edges) != 1:
        errors.append(f"Expected exactly one edge from __start__, found {len(start_edges)}")
    end_edges = [e for e in edges if e.get("to") == "__end__"]
    # 也检查条件边是否有到 __end__ 的路径
    cond_end_paths = any(
        target == "__end__"
        for ce in data.get("conditional_edges", [])
        for target in ce.get("paths", {}).values()
    )
    if not end_edges and not cond_end_paths:
        errors.append("No path leads to __end__ — workflow has no exit")

    return errors


# ── 序列化 ──

def serialize_workflow(
    name: str,
    description: str,
    nodes: list[dict],
    edges: list[dict],
    conditional_edges: Optional[list[dict]] = None,
    workflow_id: Optional[str] = None,
) -> dict:
    """Create a workflow definition dict with defaults and ID.

    Args:
        name: Workflow name.
        description: Short description.
        nodes: List of node definitions.
        edges: List of edge definitions.
        conditional_edges: Optional conditional edges.
        workflow_id: Optional ID (auto-generated if not provided).

    Returns:
        Complete workflow definition dict ready for storage.
    """
    now = datetime.now().isoformat()
    wf_id = workflow_id or f"wf_{uuid.uuid4().hex[:8]}"

    # 填充节点默认值
    filled_nodes = []
    for node in nodes:
        filled = {**NODE_DEFAULTS, **node}
        if "id" not in filled:
            filled["id"] = f"node_{uuid.uuid4().hex[:6]}"
        filled_nodes.append(filled)

    return {
        "id": wf_id,
        "name": name,
        "description": description,
        "created_at": now,
        "updated_at": now,
        "nodes": filled_nodes,
        "edges": edges,
        "conditional_edges": conditional_edges or [],
    }


def deserialize_workflow(data: dict) -> dict:
    """Load and normalize a workflow definition from JSON.

    Adds missing optional fields with defaults. Does NOT validate —
    call validate_workflow() separately if needed.

    Args:
        data: Raw dict loaded from JSON.

    Returns:
        Normalized workflow definition dict.
    """
    data.setdefault("id", "")
    data.setdefault("name", "")
    data.setdefault("description", "")
    data.setdefault("created_at", "")
    data.setdefault("updated_at", "")
    data.setdefault("nodes", [])
    data.setdefault("edges", [])
    data.setdefault("conditional_edges", [])

    for node in data["nodes"]:
        for key, default in NODE_DEFAULTS.items():
            node.setdefault(key, default)

    return data
