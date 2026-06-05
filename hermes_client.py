"""Hermes API client — shared by agent_worker, workflow_engine, and task_flow.

Provides two call modes:
  - stream_hermes(): SSE streaming, yields content chunks
  - call_hermes():   one-shot request, returns full response text
"""

import json
from typing import AsyncGenerator, Optional

import aiohttp


async def stream_hermes(
    hermes_url: str, hermes_key: str, system_prompt: str,
    user_message: str = "请回复。",
    timeout: int = 600,
) -> AsyncGenerator[str, None]:
    """Call Hermes API with streaming. Yields content chunks."""
    headers = {"Content-Type": "application/json"}
    if hermes_key:
        headers["Authorization"] = f"Bearer {hermes_key}"

    payload = {
        "model": "hermes-agent",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "stream": True,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{hermes_url}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {error_text[:200]}")

                buffer = ""
                async for chunk in resp.content.iter_any():
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("data: "):
                            data_str = line[6:]
                            if data_str == "[DONE]":
                                return
                            try:
                                data = json.loads(data_str)
                                delta = data.get("choices", [{}])[0].get("delta", {})
                                content = delta.get("content", "")
                                if content:
                                    yield content
                            except json.JSONDecodeError:
                                pass

                # Flush remaining buffer
                for line in buffer.strip().split("\n"):
                    line = line.strip()
                    if line.startswith("data: ") and line[6:] != "[DONE]":
                        try:
                            data = json.loads(line[6:])
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            pass
    except Exception as e:
        raise RuntimeError(str(e)[:200]) from e


async def call_hermes(
    hermes_url: str,
    hermes_key: str,
    messages: list[dict],
    temperature: float = 0.7,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> str:
    """One-shot Hermes API call. Returns the full response text."""
    headers = {"Content-Type": "application/json"}
    if hermes_key:
        headers["Authorization"] = f"Bearer {hermes_key}"

    payload = {
        "model": "hermes-agent",
        "messages": messages,
        "stream": False,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{hermes_url}/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {error_text[:200]}")
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(str(e)[:200]) from e
