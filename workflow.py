"""WorkflowManager — CRUD and run management for LangGraph workflows.

Replaces TaskFlowManager. Responsibilities:
- Create / read / update / delete workflow definitions
- Create / update / list run records
- AI-powered workflow generation from natural language
- Delegates execution to workflow_engine.py
"""

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from event_buffer import EventBuffer
from hermes_client import call_hermes
from storage import load_json_dir, save_json, delete_json
from workflow_serializer import serialize_workflow, deserialize_workflow, validate_workflow


class WorkflowManager:
    """Manages workflow definitions and run history."""

    def __init__(
        self,
        workflows_dir: Path,
        runs_dir: Path,
        agents: dict,
        hermes_url: str,
        hermes_key: str,
    ):
        self.workflows_dir = workflows_dir
        self.runs_dir = runs_dir
        self.agents = agents
        self.hermes_url = hermes_url
        self.hermes_key = hermes_key
        self.workflows: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self._active_buffers: dict[str, EventBuffer] = {}

    def load(self):
        """Load workflows and runs from disk."""
        self.workflows = load_json_dir(self.workflows_dir)
        self.runs = load_json_dir(self.runs_dir)

    # ── Workflow CRUD ──

    def list_workflows(self) -> list[dict]:
        return sorted(self.workflows.values(), key=lambda w: w.get("created_at", ""), reverse=True)

    def get_workflow(self, wf_id: str) -> Optional[dict]:
        return self.workflows.get(wf_id)

    def create_workflow(
        self,
        name: str,
        description: str,
        nodes: list[dict],
        edges: list[dict],
        conditional_edges: Optional[list[dict]] = None,
    ) -> dict:
        """Create a new workflow definition. Validates before saving."""
        wf = serialize_workflow(
            name=name,
            description=description,
            nodes=nodes,
            edges=edges,
            conditional_edges=conditional_edges,
        )
        errors = validate_workflow(wf, self.agents)
        if errors:
            raise ValueError(f"Invalid workflow: {'; '.join(errors)}")
        self.workflows[wf["id"]] = wf
        save_json(self.workflows_dir, wf)
        return wf

    def update_workflow(self, wf_id: str, **kwargs) -> Optional[dict]:
        """Update an existing workflow definition."""
        wf = self.workflows.get(wf_id)
        if not wf:
            return None
        if "name" in kwargs and kwargs["name"] is not None:
            wf["name"] = kwargs["name"]
        if "description" in kwargs and kwargs["description"] is not None:
            wf["description"] = kwargs["description"]
        if "nodes" in kwargs and kwargs["nodes"] is not None:
            wf["nodes"] = kwargs["nodes"]
        if "edges" in kwargs and kwargs["edges"] is not None:
            wf["edges"] = kwargs["edges"]
        if "conditional_edges" in kwargs and kwargs["conditional_edges"] is not None:
            wf["conditional_edges"] = kwargs["conditional_edges"]
        wf["updated_at"] = datetime.now().isoformat()
        # Validate after applying updates
        errors = validate_workflow(wf, self.agents)
        if errors:
            raise ValueError(f"Invalid update: {'; '.join(errors)}")
        save_json(self.workflows_dir, wf)
        return wf

    def delete_workflow(self, wf_id: str) -> bool:
        if wf_id not in self.workflows:
            return False
        del self.workflows[wf_id]
        delete_json(self.workflows_dir, wf_id)
        return True

    # ── AI 生成工作流 ──

    async def generate_workflow(self, user_description: str) -> dict:
        """Use Hermes to generate a workflow definition from natural language."""
        agent_lines = []
        for a in self.agents.values():
            preview = a.get("system_prompt", "")[:80]
            agent_lines.append(f"- {a['id']}: {a['name']} — {preview}...")
        agent_list = "\n".join(agent_lines)

        system_prompt = f"""你是一个工作流生成器。用户会描述一个多步骤任务，你需要生成一个结构化的 JSON 工作流定义。

可用的 Agent 列表：
{agent_list}

请生成一个 JSON 对象，格式如下：
{{
  "name": "工作流名称",
  "description": "简短描述",
  "nodes": [
    {{
      "id": "node_id",
      "agent_id": "agent的id",
      "name": "节点名称",
      "prompt": "给agent的指令，用 {{{{变量名}}}} 引用前序节点的输出",
      "output_var": "本节点输出的变量名"
    }}
  ],
  "edges": [
    {{"from": "__start__", "to": "第一个节点id"}},
    {{"from": "节点id", "to": "下一个节点id"}},
    {{"from": "最后一个节点id", "to": "__end__"}}
  ],
  "conditional_edges": []
}}

规则：
1. 只输出 JSON，不要其他内容
2. 第一个节点可以用 {{{{input}}}} 引用用户的原始输入
3. 后续节点用前序节点的 output_var 名引用其输出
4. 每个节点的 agent_id 必须是上面列表中的一个
5. prompt 要写清楚具体的指令，不要笼统
6. edges 必须形成从 __start__ 到 __end__ 的完整路径
7. conditional_edges 用于需要分支判断的场景，普通顺序流程留空"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_description},
        ]

        response = await call_hermes(
            self.hermes_url, self.hermes_key, messages,
            temperature=0.3, max_tokens=2000,
        )

        try:
            result = json.loads(response)
        except json.JSONDecodeError:
            m = re.search(r"```(?:json)?\s*\n?(\{.*?\})\s*```", response, re.DOTALL)
            if m:
                result = json.loads(m.group(1))
            else:
                m = re.search(r"\{[\s\S]*\"nodes\"[\s\S]*\}", response)
                if m:
                    result = json.loads(m.group(0))
                else:
                    raise ValueError(f"Hermes did not return valid JSON: {response[:200]}")

        # 填充默认值
        for i, node in enumerate(result.get("nodes", [])):
            node.setdefault("id", f"node_{i+1}")
            node.setdefault("name", f"Node {i+1}")
            node.setdefault("output_var", "")
            node.setdefault("prompt", "")
            if "agent_id" not in node:
                raise ValueError(f"Node {i+1} missing 'agent_id'")

        result.setdefault("edges", [])
        result.setdefault("conditional_edges", [])

        return result

    # ── Run 管理 ──

    def create_run(self, wf_id: str, input_text: str) -> dict:
        if wf_id not in self.workflows:
            raise ValueError(f"Workflow '{wf_id}' not found")
        run_id = uuid.uuid4().hex[:8]
        run = {
            "id": run_id,
            "workflow_id": wf_id,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "input": input_text,
            "variables": {"input": input_text},
            "node_results": {},
        }
        self.runs[run_id] = run
        save_json(self.runs_dir, run)
        return run

    def update_run(self, run_id: str, **kwargs):
        run = self.runs.get(run_id)
        if not run:
            return
        run.update(kwargs)
        save_json(self.runs_dir, run)

    def get_run_buffer(self, run_id: str) -> Optional[EventBuffer]:
        return self._active_buffers.get(run_id)

    def list_runs(self, wf_id: Optional[str] = None) -> list[dict]:
        runs = list(self.runs.values())
        if wf_id:
            runs = [r for r in runs if r["workflow_id"] == wf_id]
        return sorted(runs, key=lambda r: r.get("started_at", ""), reverse=True)
