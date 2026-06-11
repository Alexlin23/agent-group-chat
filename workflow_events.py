"""Workflow event names — single source of truth.

Backend imports from here. Frontend uses the generated JS file.
Run `python3 workflow_events.py` to regenerate the JS file.
"""


# ── Node-level events (emitted by workflow_nodes.py) ──

NODE_START = "node_start"
NODE_TEXT = "node_text"
NODE_DONE = "node_done"
NODE_ERROR = "node_error"

# ── Condition events ──

CONDITION_START = "condition_start"
CONDITION_DONE = "condition_done"
CONDITION_FALLBACK = "condition_fallback"

# ── Parallel events ──

PARALLEL_START = "parallel_start"
PARALLEL_DONE = "parallel_done"

# ── Human-in-the-loop events ──

HUMAN_INPUT_REQUIRED = "human_input_required"
HUMAN_INPUT_RECEIVED = "human_input_received"

# ── Workflow-level events (emitted by workflow_engine.py) ──

WORKFLOW_START = "workflow_start"
WORKFLOW_DONE = "workflow_done"
WORKFLOW_ERROR = "workflow_error"
WORKFLOW_PAUSED = "workflow_paused"
WORKFLOW_RESUMED = "workflow_resumed"

# ── All events (for validation) ──

ALL_EVENTS = {
    NODE_START, NODE_TEXT, NODE_DONE, NODE_ERROR,
    CONDITION_START, CONDITION_DONE, CONDITION_FALLBACK,
    PARALLEL_START, PARALLEL_DONE,
    HUMAN_INPUT_REQUIRED, HUMAN_INPUT_RECEIVED,
    WORKFLOW_START, WORKFLOW_DONE, WORKFLOW_ERROR,
    WORKFLOW_PAUSED, WORKFLOW_RESUMED,
}


def generate_js(output_path: str = None) -> str:
    """Generate a JS constants file from the Python constants.

    Args:
        output_path: If provided, write the file to this path.

    Returns:
        The generated JS content.
    """
    lines = [
        "// Auto-generated from workflow_events.py — do not edit manually",
        "// Regenerate: python3 workflow_events.py",
        "",
        "const WF_EVENTS = {",
    ]

    # Collect all module-level string constants
    import inspect
    module = inspect.getmodule(generate_js)
    for name, value in sorted(inspect.getmembers(module)):
        if isinstance(value, str) and name.isupper() and name != "ALL_EVENTS":
            lines.append(f'  {name}: "{value}",')

    lines.append("};")
    lines.append("")

    js_content = "\n".join(lines)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(js_content)

    return js_content


if __name__ == "__main__":
    import sys
    out = sys.argv[1] if len(sys.argv) > 1 else "workflow_events.js"
    js = generate_js(out)
    print(js)
    print(f"\nWritten to {out}")
