---
name: agent-group-chat
description: Project knowledge base for the Agent Group Chat codebase — multi-agent collaboration system with @mention chains and task flows
---

# Agent Group Chat — Project Knowledge

## Project Purpose

A local multi-agent group chat app where AI agents (架构师/编码者/审查者) collaborate on tasks. Users chat naturally, agents respond based on their roles, and `@mention` chains coordinate work. Includes a Task Flow system for structured multi-step agent pipelines.

## File Map

| File | Purpose |
|------|---------|
| `server.py` | FastAPI app entry — startup, route registration, health endpoint, frontend serving |
| `app_state.py` | Shared mutable state — `agents: dict` and `conversations: dict` (single source of truth) |
| `orchestrator.py` | Concurrent agent scheduling with `@mention` chain propagation (max depth 5) |
| `agent_worker.py` | Independent async unit that calls Hermes API for one agent, streams response |
| `message_bus.py` | Per-conversation message store — `MessageBus` (atomic append + persist) and `MessageBusManager` |
| `event_buffer.py` | In-memory event log with async subscription, replay support, 30s heartbeats |
| `task_flow.py` | `TaskFlowManager` — flow CRUD, AI-generated flow creation, execution management |
| `workflow_engine.py` | Executes task flow steps as linear pipelines with `{{variable}}` template rendering |
| `storage.py` | YAML agents + JSON conversations/flows — load, save, delete |
| `text_utils.py` | `@mention` parsing (CJK-aware), `build_context_text`, `strip_agent_prefix`, constants |
| `hermes_client.py` | Hermes API client — `stream_hermes()` (SSE generator) and `call_hermes()` (one-shot) |
| `models.py` | Pydantic schemas — `AgentCreate/Update`, `ConversationCreate/Update`, `MessageRequest`, `TaskFlowCreate/Update/GenerateRequest/RunRequest`, `FlowStepCreate` |
| `index.html` | Single-file frontend — CSS in `<style>`, JS inline, no framework |
| `agents.yaml` | Agent definitions — id, name, color, avatar, system_prompt |
| `start.sh` | One-click startup — creates venv, checks Hermes, launches server |
| `requirements.txt` | `fastapi`, `uvicorn`, `aiohttp`, `pyyaml`, `langgraph` |
| `routes/__init__.py` | Empty init |
| `routes/agents.py` | Agent CRUD — `GET/POST/PUT/DELETE /api/agents` |
| `routes/conversations.py` | Conversation CRUD + message + SSE stream + task status |
| `routes/task_flows.py` | Task Flow CRUD + generate + run + SSE stream |

## Key Concepts

### Agents

Defined in `agents.yaml`, loaded at startup into `app_state.agents` (dict keyed by `id`). Each agent has: `id`, `name`, `color`, `avatar`, `system_prompt`. Agents persist to YAML on create/update/delete via `storage.save_agents()`.

### Conversations

Stored as JSON files in `conversations/` directory. Each has `id`, `name`, `created_at`, `messages[]`. Loaded at startup into `app_state.conversations`. Messages are managed by `MessageBus` instances (one per conversation).

### @mention Chains

1. User message is parsed by `text_utils.parse_mentions()` — extracts `@agent_id` or `@agent_name`
2. If no mentions found, first agent is targeted by default
3. `Orchestrator.process_message()` spawns `AgentWorker` instances concurrently for target agents
4. After workers finish, `extract_mentioned_agents()` scans responses for new `@mentions`
5. Mentioned agents are queued for the next depth level (up to `MAX_MENTION_DEPTH=5`)
6. Each agent can respond at most `MAX_RESPONSES_PER_AGENT=3` times per task
7. `mention_trigger` events are pushed to EventBuffer when chain propagation occurs

### Task Flows

Structured pipelines with ordered steps. Each step targets an agent with a prompt template. Variables (`{{var_name}}`) are passed between steps. Flows can be AI-generated from natural language descriptions via `POST /api/task-flows/generate`.

Step fields: `id`, `agent_id`, `name`, `prompt_template`, `input_vars[]`, `output_var`, `timeout`.

### SSE Streaming

Frontend subscribes to `/api/conversations/{id}/stream?last_id=N`. The `EventBuffer.subscribe()` method yields events from `last_id+1`, blocking when caught up (30s heartbeat). Events: `agent_start`, `text`, `agent_done`, `mention_trigger`, `done`, `error`. For task flows: `flow_start`, `step_start`, `step_text`, `step_done`, `step_error`, `flow_error`, `flow_done`.

### Per-conversation State

Frontend caches `streamingAgents`, `lastEventId`, `convData` per conversation. SSE handlers include stale-event guards — events from a previous task are ignored after conversation switch. Backend tracks `active_tasks`, `active_buffers`, `conversation_locks` per conversation ID.

### EventBuffer

In-memory event log. `push()` appends and wakes all subscribers. `subscribe(last_id)` yields from `last_id+1`, blocks with `asyncio.Event` when caught up. `close()` marks done — subscribers drain remaining events and stop. Supports replay for refresh-safe streaming.

### MessageBus

Per-conversation shared message state. `append()` is atomic (asyncio lock). `_persist()` writes to JSON file under lock. `MessageBusManager` manages all bus instances with double-checked locking on `get_or_create()`.

## Common Modification Patterns

### Adding a new agent

Edit `agents.yaml` — add entry with `id`, `name`, `color`, `avatar`, `system_prompt`. Restart server, or use `POST /api/agents` at runtime.

### Changing agent behavior

Edit `system_prompt` field in `agents.yaml`. For runtime changes, use `PUT /api/agents/{agent_id}`.

### Adding API endpoint

1. Add route to appropriate `routes/*.py` file
2. Use `Depends(validate_conv_id)` for conversation path validation
3. Access shared state via `import app_state` (agents/conversations) or `sys.modules['__main__']` (server-level: `active_tasks`, `bus_manager`, `orchestrator`, etc.)
4. Add Pydantic model to `models.py` if needed

### Changing frontend

Edit `index.html` — it's a single file. CSS is in `<style>` at top, JS is inline at bottom. Uses event delegation (single listeners on parent containers). DOM IDs follow `#element-name` convention.

### Adding new event type

1. Push event in `orchestrator.py` or `agent_worker.py` via `event_buffer.push("event_name", {...})`
2. For task flows, push in `workflow_engine.py`
3. Handle in `index.html` SSE listener — add case in the event type switch

### Adding new Task Flow step behavior

Modify `workflow_engine.py:execute_step()` to customize step execution. Template rendering is in `_render_template()`.

## Gotchas

### app_state.py holds shared state

`app_state.py` exists because of `__main__` vs import ambiguity. When `server.py` runs as `__main__`, its module-level variables are in `sys.modules['__main__']`. But route modules import `server`, creating a different module object. `app_state.py` solves this — both `__main__` and imported modules see the same `app_state.agents` and `app_state.conversations`.

### Routes use sys.modules['__main__'] for server-level state

Routes access `active_tasks`, `active_buffers`, `conversation_locks`, `bus_manager`, `orchestrator`, `task_flow_manager`, `DATA_DIR`, `AGENTS_FILE` via `sys.modules['__main__']`. This is because these are defined in `server.py` module scope, not in `app_state.py`.

### validate_conv_id regex

`^[0-9a-f]{12}$` — matches the first 12 hex chars of `uuid4().hex`. Conversation IDs are generated by `uuid.uuid4().hex[:12]`.

### Refactored shared code

`agent_worker.py` and the old `graph.py` previously duplicated `stream_hermes`, `parse_mentions`, etc. These are now consolidated into `text_utils.py` and `hermes_client.py`. The old `graph.py` was replaced by `workflow_engine.py`.

### Input box hasManualHeight flag

The frontend input textarea has a drag-resize handle. When the user drags to resize, `hasManualHeight` is set to `true`, preventing `autoResize()` from overriding the manual height. Reset only when conversation switches.

### Auto-rename triggers on "新对话"

When `conv.name === "新对话"` and the first user message arrives, the frontend auto-generates a name by calling `PUT /api/conversations/{id}` before sending the message. The PUT is awaited before the message POST proceeds.

### Conversation persistence triggers

`save_conversation()` is called on:
- Conversation create
- Conversation update (name change)
- User message arrival (after `bus.append()`)
- Agent response completion (after orchestrator finishes, in the background task)

### EventBuffer lifecycle

One EventBuffer per active task. Created when message is sent (`POST /message`), stored in `active_buffers[conv_id]`. Closed when orchestrator finishes or on error. Cleaned up in the `_run()` finally block. Old buffer is replaced if a new message arrives during processing.
