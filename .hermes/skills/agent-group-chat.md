---
name: agent-group-chat
description: How to use the Agent Group Chat system — agents, task flows, and interaction patterns
---

# Agent Group Chat

A multi-agent group chat system where AI agents collaborate on tasks through natural conversation.

## Agents

Agents are defined with an ID, name, role description, and behavior rules. Each agent responds independently based on its role.

**Triggering agents:**
- Use `@agent-name` or `@agent-id` in your message to target specific agents
- Without @mention, the system picks: per-conversation default > global default > first agent
- An agent mentioned in another agent's response will be auto-triggered (up to depth 5)

**Default agent:**
- ★ star on agent list = global default (all conversations)
- ⋯ menu on conversation = per-conversation override (higher priority)

**Streaming:**
- Responses stream token-by-token in real-time
- Sending a new message does not interrupt in-progress responses
- Only @mention propagation is stopped on new messages

**Conversations:**
- Auto-saved after each message
- Name auto-generated from first message
- Click ⋯ on a conversation for rename, delete, or set default agent

## Task Flows

Task Flows are multi-step agent pipelines for complex tasks requiring multiple agents in sequence.

**Structure:** A flow has ordered steps. Each step:
- Targets an agent by ID
- Has a prompt template (supports `{{variable}}` placeholders)
- Stores output in a named variable for later steps

**How to use:**
1. Describe a complex multi-step task in conversation
2. An agent proposes a flow structure (which agents, what order, what each does)
3. The flow can be created and executed via API

**Flow execution:**
- Runs via LangGraph StateGraph
- Agents process sequentially
- Each step receives conversation history + rendered template
- Variables from previous steps are available via `{{variable_name}}`

**Managing flows:**
- Sidebar "Task Flows" section shows all flows
- Click to view details or run with input
- Delete with ✕ button
