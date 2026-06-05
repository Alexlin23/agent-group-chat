"""Text utilities — shared by agent_worker, workflow_engine, task_flow, and server.

Consolidates all @mention parsing, context building, and text cleanup
into one authoritative module.
"""

import re


def build_context_text(messages: list[dict], agents: dict) -> str:
    """Build a formatted conversation history string for agent prompts."""
    lines = []
    for msg in messages:
        ts = ''
        if msg.get('timestamp'):
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(msg['timestamp'])
                ts = f" [{dt.strftime('%H:%M')}]"
            except (ValueError, TypeError):
                pass
        if msg["role"] == "user":
            lines.append(f"[用户{ts}]: {msg['content']}")
        elif msg["role"] == "assistant":
            aname = agents.get(msg.get("agent_id", ""), {}).get("name", "Agent")
            lines.append(f"[{aname}{ts}]: {msg['content']}")
    return "\n\n".join(lines) if lines else "(暂无对话历史)"


def strip_agent_prefix(text: str, agent_name: str) -> str:
    """Remove [AgentName]: or [AgentName] : prefix from response text."""
    for prefix in [f"[{agent_name}]:", f"[{agent_name}] :"]:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def _is_word_boundary(ch: str) -> bool:
    """Check if a character is a word boundary (not alphanumeric, not CJK)."""
    if not ch:
        return True
    if ch.isalnum():
        return False
    # CJK Unified Ideographs range
    if '\u4e00' <= ch <= '\u9fff':
        return False
    # CJK Extension A
    if '\u3400' <= ch <= '\u4dbf':
        return False
    return True


def extract_mentioned_agents(text: str, agents: dict) -> list[str]:
    """Extract @mentioned agent IDs from text.

    Handles both agent names and agent IDs as @mentions.
    Uses proper boundary checking for CJK characters.
    """
    if not text or not agents:
        return []

    # Build tag list: (tag_string, agent_id), sorted longest-first
    all_tags: list[tuple[str, str]] = []
    for a in agents.values():
        all_tags.append((a["name"], a["id"]))
        all_tags.append((a["id"], a["id"]))
    all_tags.sort(key=lambda x: len(x[0]), reverse=True)

    found_ids: list[str] = []
    idx = 0
    while idx < len(text):
        if text[idx] == "@":
            matched = False
            for tag, aid in all_tags:
                end = idx + 1 + len(tag)
                if text[idx + 1:end] == tag:
                    # Check boundary: next char must not be alphanumeric or CJK
                    next_ch = text[end] if end < len(text) else ""
                    if _is_word_boundary(next_ch):
                        if aid not in found_ids:
                            found_ids.append(aid)
                        idx = end
                        matched = True
                        break
            if not matched:
                idx += 1
        else:
            idx += 1

    return found_ids


def parse_mentions(text: str, agent_list: list[dict]) -> tuple[list[str], str]:
    """Parse @mentions from user input text.

    Returns (matched_agent_ids, cleaned_text_with_mentions_removed).
    Used by server.py for user message parsing.
    """
    name_to_id: dict[str, str] = {}
    for a in agent_list:
        name_to_id[a["name"]] = a["id"]
        name_to_id[a["id"]] = a["id"]

    pattern = r"@(\S+)"
    found_ids: list[str] = []
    for match in re.finditer(pattern, text):
        tag = match.group(1)
        if tag in name_to_id and name_to_id[tag] not in found_ids:
            found_ids.append(name_to_id[tag])

    if not found_ids:
        return [], text

    # Remove matched mentions from text (reverse order to preserve indices)
    cleaned = text
    for match in reversed(list(re.finditer(pattern, text))):
        tag = match.group(1)
        if tag in name_to_id:
            cleaned = cleaned[:match.start()] + cleaned[match.end():]
    cleaned = cleaned.strip()

    return found_ids, cleaned


# Shared constants
MAX_RESPONSES_PER_AGENT = 3
MAX_MENTION_DEPTH = 5
