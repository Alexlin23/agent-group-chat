# Agent Group Chat v2

A local multi-agent group chat application where AI agents collaborate on tasks through role-based interaction. Users chat naturally, agents respond based on their defined roles, and `@mention` chains coordinate work across agents.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Frontend (index.html)                                      │
│  Single-file vanilla JS, dark/light theme, SSE streaming    │
└──────────────────────┬──────────────────────────────────────┘
                       │ REST + SSE
┌──────────────────────▼──────────────────────────────────────┐
│  FastAPI Backend (server.py)                                │
│  ┌──────────────┐ ┌──────────────────┐ ┌──────────────────┐ │
│  │ routes/       │ │ routes/           │ │ routes/           │ │
│  │ agents.py     │ │ conversations.py  │ │ task_flows.py     │ │
│  └──────────────┘ └──────────────────┘ └──────────────────┘ │
│  ┌──────────────┐ ┌──────────────────┐ ┌──────────────────┐ │
│  │ app_state.py  │ │ orchestrator.py   │ │ agent_worker.py   │ │
│  │ (shared dict) │ │ (@mention chains) │ │ (Hermes API call) │ │
│  └──────────────┘ └──────────────────┘ └──────────────────┘ │
│  ┌──────────────┐ ┌──────────────────┐ ┌──────────────────┐ │
│  │ message_bus.py│ │ event_buffer.py   │ │ task_flow.py      │ │
│  │ (per-conv msg)│ │ (SSE pub/sub)     │ │ (flow CRUD+exec)  │ │
│  └──────────────┘ └──────────────────┘ └──────────────────┘ │
│  ┌──────────────┐ ┌──────────────────┐ ┌──────────────────┐ │
│  │ storage.py    │ │ text_utils.py     │ │ hermes_client.py  │ │
│  │ (YAML + JSON) │ │ (@mention parse)  │ │ (API client)      │ │
│  └──────────────┘ └──────────────────┘ └──────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐│
│  │ workflow_engine.py — Task Flow step executor              ││
│  └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                       │
         ┌─────────────▼─────────────┐
         │  Hermes API Server (:8642) │
         │  OpenAI-compatible LLM API │
         └───────────────────────────┘
```

### Core Modules

| Module | Purpose |
|--------|---------|
| `server.py` | FastAPI app, startup, route registration, health endpoint, frontend serving |
| `app_state.py` | Shared mutable state — `agents` and `conversations` dicts |
| `routes/agents.py` | Agent CRUD (list, get, create, update, delete) |
| `routes/conversations.py` | Conversation CRUD + message send + SSE stream + task status |
| `routes/task_flows.py` | Task Flow CRUD + generate + run + SSE stream |
| `orchestrator.py` | Concurrent agent scheduling, `@mention` chain propagation (max depth 5) |
| `agent_worker.py` | Independent async unit that calls Hermes API for one agent |
| `message_bus.py` | Per-conversation message store with atomic append and persistence |
| `event_buffer.py` | In-memory event log with async subscription, replay support, heartbeats |
| `task_flow.py` | TaskFlowManager — flow CRUD, Hermes-generated flow creation, execution |
| `workflow_engine.py` | Executes task flow steps as linear pipelines with `{{variable}}` passing |
| `storage.py` | YAML agents + JSON conversations/flows load/save/delete |
| `text_utils.py` | `@mention` parsing (CJK-aware), context building, agent prefix stripping |
| `hermes_client.py` | Hermes API client — `stream_hermes()` (SSE) and `call_hermes()` (one-shot) |
| `models.py` | Pydantic request/response schemas |
| `index.html` | Single-file frontend — CSS + JS, no framework |

## How to Run

### Prerequisites

- Python 3.12+
- Hermes API Server running on `http://127.0.0.1:8642` (OpenAI-compatible `/v1/chat/completions` endpoint)

### Quick Start

```bash
chmod +x start.sh
./start.sh
```

The script will:
1. Create a virtualenv if needed and install dependencies
2. Check Hermes API Server connectivity
3. Start the server on `http://localhost:8081`

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HERMES_API_URL` | `http://127.0.0.1:8642` | Hermes API Server URL |
| `HERMES_API_KEY` | `$API_SERVER_KEY` or `local-chat` | API authentication key |
| `CHAT_SERVER_PORT` | `8081` | Server listen port |

### Manual Start

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python server.py
```

### Systemd Service

The server can run as a systemd service. Configure `HERMES_API_URL`, `HERMES_API_KEY`, and `CHAT_SERVER_PORT` in the service environment.

## API Endpoints

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Server health + Hermes connectivity + stats |

### Agents

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/agents` | List all agents |
| `GET` | `/api/agents/{agent_id}` | Get agent by ID |
| `POST` | `/api/agents` | Create agent |
| `PUT` | `/api/agents/{agent_id}` | Update agent |
| `DELETE` | `/api/agents/{agent_id}` | Delete agent |

### Conversations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/conversations` | List conversations (sorted by created_at) |
| `POST` | `/api/conversations` | Create conversation |
| `GET` | `/api/conversations/{conv_id}` | Get conversation with messages |
| `PUT` | `/api/conversations/{conv_id}` | Update conversation name |
| `DELETE` | `/api/conversations/{conv_id}` | Delete conversation and cancel active tasks |
| `POST` | `/api/conversations/{conv_id}/message` | Send message, start agent processing (returns immediately) |
| `GET` | `/api/conversations/{conv_id}/stream` | SSE stream — subscribe to agent events |
| `GET` | `/api/conversations/{conv_id}/task_status` | Check task status (idle/running/done) |

### Task Flows

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/task-flows` | List all task flows |
| `POST` | `/api/task-flows` | Create task flow |
| `POST` | `/api/task-flows/generate` | Generate flow from natural language via Hermes |
| `GET` | `/api/task-flows/{flow_id}` | Get task flow |
| `PUT` | `/api/task-flows/{flow_id}` | Update task flow |
| `DELETE` | `/api/task-flows/{flow_id}` | Delete task flow |
| `POST` | `/api/task-flows/{flow_id}/run` | Execute task flow (returns immediately) |
| `GET` | `/api/task-flows/{flow_id}/runs` | List runs for a flow |
| `GET` | `/api/task-flows/runs/{run_id}/stream` | SSE stream for flow execution events |

### Path Validation

Conversation IDs must match `^[0-9a-f]{12}$` (first 12 hex chars of uuid4). Enforced via `validate_conv_id` dependency.

## Agents Configuration

Agents are defined in `agents.yaml`:

```yaml
agents:
- id: architect
  name: 架构师
  color: '#EC407A'
  avatar: '🏗'
  system_prompt: |
    你是系统架构师（Architect），专注于整体设计、模块划分和接口定义。
    ...
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier, used in `@mention` targeting |
| `name` | string | Display name, also usable in `@mention` |
| `color` | string | Hex color for UI avatar/badge |
| `avatar` | string | Emoji avatar |
| `system_prompt` | string | LLM system prompt defining the agent's role and behavior |

### @mention Rules

- Users can `@agent_id` or `@agent_name` in messages to target specific agents
- If no `@mention` is found, the first agent responds by default
- Agent responses can `@mention` other agents, triggering a chain
- Chain propagation: max depth 5, max 3 responses per agent per task
- CJK character boundaries are handled correctly in mention parsing

## Task Flows

Task Flows are structured multi-step agent pipelines. Each step targets an agent with a prompt template, and variables are passed between steps.

### Step Format

```json
{
  "id": "step_1",
  "agent_id": "architect",
  "name": "需求分析",
  "prompt_template": "分析以下需求并给出架构方案：\n\n{{input}}",
  "input_vars": ["input"],
  "output_var": "architecture_plan",
  "timeout": 300
}
```

### Variable Passing

- First step can reference `{{input}}` (the user's original input)
- Subsequent steps reference previous steps' `output_var` names
- Templates use `{{variable_name}}` syntax

### Creating Flows

**Manual:** `POST /api/task-flows` with steps array.

**AI-Generated:** `POST /api/task-flows/generate` with a natural language description. Hermes will produce a structured flow definition that can be inspected before execution.

### Executing Flows

`POST /api/task-flows/{flow_id}/run` with `input_text`. Returns immediately with a `run_id`. Subscribe to `GET /api/task-flows/runs/{run_id}/stream` for SSE events.

### SSE Event Types (Task Flow)

| Event | Description |
|-------|-------------|
| `flow_start` | Flow execution began |
| `step_start` | Step began executing |
| `step_text` | Streaming text chunk from agent |
| `step_done` | Step completed with full response |
| `step_error` | Step failed |
| `flow_error` | Flow execution failed |
| `flow_done` | Flow completed with final variables |

## Frontend Features

- **Conversation switching** — Sidebar lists conversations; click to switch, messages load instantly
- **Per-conversation streaming state** — `streamingAgents`, `lastEventId`, `convData` are cached per conversation; stale-event guards on SSE handlers prevent cross-talk
- **Auto-rename** — When a conversation named "新对话" receives its first message, the frontend auto-generates a name via PUT before proceeding
- **Drag-resizable input box** — Drag handle on top edge of input area; `hasManualHeight` flag prevents `autoResize` from overriding drag-set height
- **Dark/Light theme** — CSS custom properties toggle via `[data-theme="light"]` on `<html>`; persisted in localStorage
- **Toast notifications** — Non-blocking feedback for errors and actions
- **@mention autocomplete** — Type `@` to see agent suggestions with avatar, name, and ID
- **Agent management** — Create/edit/delete agents via modal dialogs with color picker
- **Task Flow management** — Create, generate, edit, run, and monitor task flows
- **Markdown rendering** — Agent responses support code blocks, lists, headings, blockquotes
- **Event delegation** — Single listeners on parent containers for performant UI interaction
- **Streaming cursor** — Blinking `▋` indicator on active streaming messages

## Tech Stack

- **Backend:** Python 3.12, FastAPI, asyncio, Pydantic, PyYAML, aiohttp
- **Frontend:** Vanilla JavaScript (no framework), single-file HTML with inline CSS/JS
- **Streaming:** Server-Sent Events (SSE) with replay support
- **Storage:** YAML (agents), JSON files (conversations, task flows, runs)
- **LLM API:** OpenAI-compatible `/v1/chat/completions` via Hermes API Server

## Project Structure

```
agent-group-chat/
├── server.py              # FastAPI app entry point
├── app_state.py           # Shared mutable state
├── orchestrator.py        # Agent scheduling + @mention chains
├── agent_worker.py        # Per-agent async processing
├── message_bus.py         # Per-conversation message store
├── event_buffer.py        # SSE event buffer with pub/sub
├── task_flow.py           # TaskFlowManager (CRUD + execution)
├── workflow_engine.py     # Task flow step executor
├── storage.py             # YAML/JSON file I/O
├── text_utils.py          # @mention parsing, context building
├── hermes_client.py       # Hermes API client
├── models.py              # Pydantic schemas
├── agents.yaml            # Agent definitions
├── index.html             # Single-file frontend
├── start.sh               # One-click startup script
├── requirements.txt       # Python dependencies
├── routes/
│   ├── __init__.py
│   ├── agents.py          # Agent CRUD routes
│   ├── conversations.py   # Conversation + message + SSE routes
│   └── task_flows.py      # Task flow routes
├── conversations/         # JSON conversation files (gitignored)
├── task_flows/            # JSON flow definitions (gitignored)
└── task_flow_runs/        # JSON run records (gitignored)
```
