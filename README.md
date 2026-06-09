# Agent Group Chat

多 Agent 群聊系统，AI 代理通过角色协作完成任务。用户自然对话，Agent 按角色回复，`@mention` 链协调跨 Agent 工作。

## 功能

- **群聊** — 多个 Agent 在同一对话中协作，用 `@名字` 指定回复
- **Agent 管理** — 动态添加/编辑/删除 Agent，每个 Agent 有独立角色和提示词
- **Task Flow 工作流** — 多 Agent 流水线，步骤间变量传递，支持 AI 自动生成
- **流式输出** — SSE 实时推送，边生成边显示
- **@mention 链** — Agent 回复中 @其他 Agent 会自动触发后续回复

## 架构

```
┌─────────────────────────────────────────┐
│  前端 (index.html)                       │
│  单文件 Vanilla JS，深色/浅色主题，SSE    │
└────────────────┬────────────────────────┘
                 │ REST + SSE
┌────────────────▼────────────────────────┐
│  FastAPI 后端 (server.py :8081)          │
│  ┌────────────┐ ┌─────────────────────┐ │
│  │ Agent CRUD  │ │ 对话 + 消息 + SSE    │ │
│  └────────────┘ └─────────────────────┘ │
│  ┌────────────┐ ┌─────────────────────┐ │
│  │ 调度器      │ │ Agent Worker         │ │
│  │ @mention链  │ │ Hermes API 调用      │ │
│  └────────────┘ └─────────────────────┘ │
│  ┌────────────┐ ┌─────────────────────┐ │
│  │ 工作流引擎  │ │ 消息总线 + 事件缓冲   │ │
│  └────────────┘ └─────────────────────┘ │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│  Hermes API Server (:8642)               │
│  OpenAI 兼容 LLM API                     │
└─────────────────────────────────────────┘
```

## 快速开始

```bash
# 前置条件：Hermes API Server 运行在 http://127.0.0.1:8642

chmod +x start.sh
./start.sh
```

访问 http://localhost:8081。

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HERMES_API_URL` | `http://127.0.0.1:8642` | Hermes API 地址 |
| `HERMES_API_KEY` | `$API_SERVER_KEY` | API 认证密钥 |
| `CHAT_SERVER_PORT` | `8081` | 服务端口 |

### 手动启动

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python server.py
```

## Agent 配置

Agent 定义在 `agents.yaml`：

```yaml
agents:
- id: architect
  name: 架构师
  color: '#EC407A'
  avatar: '🏗'
  description: 系统架构师，负责方案设计、模块划分和接口定义
  system_prompt: |
    你是系统架构师（Architect），专注于整体设计、模块划分和接口定义。
    ...
```

| 字段 | 说明 |
|------|------|
| `id` | 唯一标识，用于 @mention |
| `name` | 显示名称 |
| `description` | 给其他 Agent 看的一句话简介 |
| `system_prompt` | Agent 的角色定义和行为规则 |

提示词组装时，系统自动注入其他 Agent 的 `description`，无需手动写协作信息。

## Task Flow 工作流

多 Agent 流水线，步骤间通过 `{{变量名}}` 传递数据。

**创建方式：**
- API 直接创建：`POST /api/task-flows`
- AI 生成：`POST /api/task-flows/generate`（传入自然语言描述）

**执行：**
- `POST /api/task-flows/{id}/run`（传入输入内容）
- SSE 流式监听执行事件

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/agents` | 列出所有 Agent |
| `POST` | `/api/agents` | 创建 Agent |
| `PUT` | `/api/agents/{id}` | 编辑 Agent |
| `DELETE` | `/api/agents/{id}` | 删除 Agent |
| `GET` | `/api/conversations` | 列出对话 |
| `POST` | `/api/conversations` | 创建对话 |
| `POST` | `/api/conversations/{id}/message` | 发送消息 |
| `GET` | `/api/conversations/{id}/stream` | SSE 流式事件 |
| `GET` | `/api/task-flows` | 列出工作流 |
| `POST` | `/api/task-flows` | 创建工作流 |
| `POST` | `/api/task-flows/generate` | AI 生成工作流 |
| `POST` | `/api/task-flows/{id}/run` | 执行工作流 |

## 技术栈

- **后端：** Python 3.12, FastAPI, asyncio, Pydantic, LangGraph
- **前端：** Vanilla JS，单文件 HTML
- **流式：** Server-Sent Events (SSE)
- **存储：** YAML (Agent) + JSON (对话/工作流)
- **LLM：** OpenAI 兼容 API (Hermes)

## 项目结构

```
agent-group-chat/
├── server.py              # FastAPI 入口
├── orchestrator.py        # Agent 调度 + @mention 链
├── agent_worker.py        # 单 Agent 异步处理
├── message_bus.py         # 对话消息管理
├── event_buffer.py        # SSE 事件缓冲
├── task_flow.py           # 工作流管理
├── workflow_engine.py     # 工作流执行引擎
├── storage.py             # YAML/JSON 持久化
├── text_utils.py          # @mention 解析
├── hermes_client.py       # Hermes API 客户端
├── models.py              # Pydantic 模型
├── agents.yaml            # Agent 定义
├── index.html             # 前端
├── routes/                # API 路由
├── conversations/         # 对话数据 (gitignored)
└── task_flows/            # 工作流定义 (gitignored)
```

[English](README_EN.md)
