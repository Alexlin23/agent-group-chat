"""Code executor — safe exec() for workflow code nodes.

Provides a sandboxed(ish) Python code execution environment.
Variables from the workflow are injected into the code's namespace.
The code can set `result` to control the output, otherwise stdout is captured.

This is a LOCAL tool — no network sandboxing. The main protections are:
- Whitelisted builtins (restricted __import__ — only safe modules)
- Timeout via asyncio
- Output size cap
"""

import io
import sys
import traceback
from typing import Any


# ── Safe modules (pre-imported, no __import__ needed) ──

import json as _json
import math as _math
import re as _re
import datetime as _datetime
import time as _time

SAFE_MODULES = {
    "json": _json,
    "math": _math,
    "re": _re,
    "datetime": _datetime,
    "time": _time,
}


def _safe_import(name, *args, **kwargs):
    """Restricted __import__ — only allows SAFE_MODULES."""
    if name in SAFE_MODULES:
        return SAFE_MODULES[name]
    raise ImportError(f"Import of '{name}' is not allowed. Available: {', '.join(SAFE_MODULES.keys())}")


# ── Safe builtins whitelist ──

SAFE_BUILTINS = {
    # 类型
    "bool": bool, "int": int, "float": float, "str": str,
    "list": list, "dict": dict, "tuple": tuple, "set": set,
    "bytes": bytes, "bytearray": bytearray,
    # 数学
    "abs": abs, "min": min, "max": max, "sum": sum,
    "round": round, "pow": pow, "divmod": divmod,
    "range": range, "enumerate": enumerate, "zip": zip,
    "map": map, "filter": filter,
    # 序列
    "len": len, "sorted": sorted, "reversed": reversed,
    "any": any, "all": all,
    # 转换
    "chr": chr, "ord": ord, "hex": hex, "oct": oct, "bin": bin,
    "repr": repr, "hash": hash,
    # 类型检查（只保留安全的）
    "isinstance": isinstance, "issubclass": issubclass,
    "callable": callable,
    # IO
    "print": print,
    # 杂项（受限 __import__，不含 getattr/setattr/vars/type）
    "__import__": _safe_import,
    "True": True, "False": False, "None": None,
}

MAX_OUTPUT_SIZE = 100_000  # 100KB cap on stdout


def execute_code(code: str, variables: dict[str, Any]) -> dict:
    """Execute Python code with variable injection.

    Args:
        code: Python code string to execute.
        variables: Workflow variables dict (mutable, changes are preserved).

    Returns:
        {
            "result": Any,       # The value of `result` variable, or stdout
            "stdout": str,       # Captured stdout
            "error": str | None, # Error message if failed
            "traceback": str,    # Full traceback if failed
        }
    """
    globals_dict = {
        "__builtins__": SAFE_BUILTINS,
        **SAFE_MODULES,
    }
    locals_dict = {
        "variables": variables,
        "result": None,
    }

    old_stdout = sys.stdout
    captured = io.StringIO()
    sys.stdout = captured

    error = None
    tb = None

    try:
        exec(code, globals_dict, locals_dict)
    except Exception as e:
        error = str(e)
        tb = traceback.format_exc()
    finally:
        sys.stdout = old_stdout

    stdout = captured.getvalue()
    if len(stdout) > MAX_OUTPUT_SIZE:
        stdout = stdout[:MAX_OUTPUT_SIZE] + f"\n... (truncated, {len(stdout)} chars total)"

    result = locals_dict.get("result")
    if result is None and not error:
        result = stdout.strip()

    return {
        "result": result,
        "stdout": stdout,
        "error": error,
        "traceback": tb,
    }
