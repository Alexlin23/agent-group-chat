"""Workflow engine for Task Flow execution.

Replaces the old graph.py. Uses shared hermes_client and text_utils.
Executes task flows as strict linear pipelines with variable passing.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from event_buffer import EventBuffer
from hermes_client import stream_hermes
from text_utils import build_context_text, strip_agent_prefix

_SKILL_CACHE = None


def _load_project_skill() -> str:
    """Load the project skill file (cached)."""
    global _SKILL_CACHE
    if _SKILL_CACHE is not None:
        return _SKILL_CACHE
    skill_path = Path(__file__).parent / ".hermes" / "skills" / "agent-group-chat.md"
    if skill_path.exists():
        _SKILL_CACHE = skill_path.read_text(encoding="utf-8") + "\n\n"
    else:
        _SKILL_CACHE = ""
    return _SKILL_CACHE


def _render_template(template: str, variables: dict[str, str]) -> str:
    """Render a {{var}} template with variables."""
    def replacer(match):
        var_name = match.group(1).strip()
        return variables.get(var_name, match.group(0))
    return re.sub(r'\{\{(.+?)\}\}', replacer, template)


async def execute_step(
    step: dict,
    agent: dict,
    variables: dict[str, str],
    messages: list[dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer: EventBuffer,
) -> str:
    """Execute a single task flow step. Returns the agent's response text."""
    step_id = step["id"]
    step_name = step.get("name", step_id)

    event_buffer.push("step_start", {
        "step_id": step_id,
        "step_name": step_name,
        "agent_id": agent["id"],
        "agent_name": agent["name"],
    })

    # Render the prompt template with current variables
    prompt_template = step.get("prompt_template", "")
    rendered_prompt = _render_template(prompt_template, variables)

    # Build full agents dict for context builder
    agents_dict = {agent["id"]: agent}
    context_text = build_context_text(messages, agents_dict)

    system_prompt = (
        f"{agent.get('system_prompt', '')}\n\n"
        f"{_load_project_skill()}"
        f"---\n任务步骤: {step_name}\n---\n\n"
        f"{rendered_prompt}"
    )

    # Stream response
    full_response = ""
    try:
        async for chunk in stream_hermes(hermes_url, hermes_key, system_prompt):
            full_response += chunk
            event_buffer.push("step_text", {
                "step_id": step_id,
                "agent_id": agent["id"],
                "text": chunk,
            })
    except RuntimeError as e:
        event_buffer.push("step_error", {
            "step_id": step_id,
            "agent_id": agent["id"],
            "error": str(e),
        })
        raise

    # Clean and emit completion
    clean_response = strip_agent_prefix(full_response, agent["name"])

    event_buffer.push("step_done", {
        "step_id": step_id,
        "step_name": step_name,
        "agent_id": agent["id"],
        "agent_name": agent["name"],
        "output_var": step.get("output_var", ""),
        "full_response": clean_response,
    })

    return clean_response


async def execute_flow(
    flow: dict,
    input_text: str,
    agents: dict[str, dict],
    hermes_url: str,
    hermes_key: str,
    event_buffer: EventBuffer,
) -> dict[str, str]:
    """Execute a complete task flow. Returns final variables dict."""
    steps = flow.get("steps", [])
    variables: dict[str, str] = {"input": input_text}
    messages: list[dict] = [{"role": "user", "content": input_text}]

    event_buffer.push("flow_start", {
        "flow_id": flow["id"],
        "flow_name": flow.get("name", ""),
        "total_steps": len(steps),
    })

    for i, step in enumerate(steps):
        agent_id = step["agent_id"]
        agent = agents.get(agent_id)
        if not agent:
            event_buffer.push("step_error", {
                "step_id": step["id"],
                "error": f"Agent '{agent_id}' not found",
            })
            event_buffer.push("flow_error", {
                "error": f"Step {i+1} failed: agent '{agent_id}' not found",
            })
            event_buffer.close()
            return variables

        try:
            response = await execute_step(
                step=step,
                agent=agent,
                variables=variables,
                messages=messages,
                hermes_url=hermes_url,
                hermes_key=hermes_key,
                event_buffer=event_buffer,
            )

            # Store output in variables
            output_var = step.get("output_var", "")
            if output_var:
                variables[output_var] = response

            # Add to message history for context
            messages.append({
                "role": "assistant",
                "content": response,
                "agent_id": agent_id,
            })

        except Exception as e:
            event_buffer.push("flow_error", {
                "error": f"Step {i+1} ({step.get('name', '')}) failed: {str(e)[:200]}",
            })
            event_buffer.close()
            return variables

    event_buffer.push("flow_done", {
        "flow_id": flow["id"],
        "variables": {k: v[:200] + "..." if len(v) > 200 else v for k, v in variables.items()},
    })
    event_buffer.close()
    return variables
