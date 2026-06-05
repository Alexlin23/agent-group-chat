"""File-based storage — agents (YAML) and conversations/flows (JSON)."""

import json
from pathlib import Path
from typing import Optional

import yaml


def load_agents(agents_file: Path) -> dict[str, dict]:
    """Load agents from YAML file."""
    if not agents_file.exists():
        return {}
    with open(agents_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    result = {}
    for a in data.get("agents", []):
        aid = a["id"]
        result[aid] = {
            "id": aid,
            "name": a.get("name", aid),
            "color": a.get("color", "#888888"),
            "avatar": a.get("avatar", "🤖"),
            "system_prompt": a.get("system_prompt", ""),
        }
    return result


def save_agents(agents_file: Path, agents: dict[str, dict]):
    """Save agents to YAML file."""
    data = {"agents": list(agents.values())}
    with open(agents_file, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def load_conversations(data_dir: Path) -> dict[str, dict]:
    """Load all conversations from JSON files."""
    result = {}
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        return result
    for fp in data_dir.glob("*.json"):
        with open(fp, "r", encoding="utf-8") as f:
            conv = json.load(f)
            result[conv["id"]] = conv
    return result


def save_conversation(data_dir: Path, conv: dict):
    """Save a single conversation to its JSON file."""
    data_dir.mkdir(parents=True, exist_ok=True)
    fp = data_dir / f"{conv['id']}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(conv, f, ensure_ascii=False, indent=2)


def delete_conversation_file(data_dir: Path, conv_id: str):
    """Delete a conversation JSON file."""
    fp = data_dir / f"{conv_id}.json"
    if fp.exists():
        fp.unlink()


# ── Task Flow storage ─────────────────────────────────────────────────────────

def load_json_dir(data_dir: Path) -> dict[str, dict]:
    """Load all JSON files from a directory, keyed by 'id' field."""
    result = {}
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        return result
    for fp in data_dir.glob("*.json"):
        with open(fp, "r", encoding="utf-8") as f:
            item = json.load(f)
            result[item["id"]] = item
    return result


def save_json(data_dir: Path, item: dict):
    """Save a dict as JSON to data_dir/{item['id']}.json"""
    data_dir.mkdir(parents=True, exist_ok=True)
    fp = data_dir / f"{item['id']}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(item, f, ensure_ascii=False, indent=2)


def delete_json(data_dir: Path, item_id: str):
    """Delete a JSON file by ID."""
    fp = data_dir / f"{item_id}.json"
    if fp.exists():
        fp.unlink()
