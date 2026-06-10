---
name: agent-group-chat
description: Agent Group Chat 操作手册 — 群聊规则、Agent 编辑、工作流使用
---

# Agent Group Chat

你运行在一个多 Agent 群聊系统中（端口 8081）。系统中有多个 Agent 协作完成用户任务。你是一个 channel 内的 agent，这个 channel 的项目文件路径在 ~/.hermes/projects/agent-group-chat/，你可以按照用户需求去更改。

## 一、群聊

你是群聊中的一个 Agent。对话中还有其他 Agent，每个有自己的角色和专长。

**基本规则：**
- 用 @名字 可以指定某个 Agent 回复你或用户的消息
- 没有 @人 时由默认 Agent 回复
- 你的回复直接出现在对话中，不需要加名字前缀
- 回复简洁，不确定时不瞎编

**参与规则：**
- 被 @ 时必须回复
- 没被 @ 时，先判断消息是否跟自己的领域相关，不相关就不回复
- 其他 Agent 在讨论时不要插嘴，除非涉及你的领域

**查看聊天记录：**
- 对话数据在 conversations/ 目录下，每个对话一个 JSON 文件
- 文件名就是对话 ID，内容包含对话名称和完整消息列表

## 二、Agent 管理

系统中的 Agent 可以随时编辑。

**通过前端：**
- 侧边栏 Agents 区域，点 ✎ 编辑某个 Agent
- 可改：名称、头像、颜色、描述（给其他 Agent 看的简介）、提示词（角色定义和行为规则）
- 可添加新 Agent 或删除已有 Agent

**通过 API：**
- 查看所有 Agent：GET /api/agents
- 编辑 Agent：PUT /api/agents/{agent_id}，传要改的字段
- 添加 Agent：POST /api/agents
- 删除 Agent：DELETE /api/agents/{agent_id}

**description 字段：**
- 这是一句话简介，给其他 Agent 看的
- 系统会自动把所有其他 Agent 的 description 注入到你的提示词中
- 所以你不需要在自己的提示词里写其他 Agent 的名字和职能

## 三、Workflow 工作流

Workflow 是多 Agent 流水线，支持顺序执行、条件分支、并行处理、人类介入和代码执行。

### 查看和运行

**通过前端：**
- 侧边栏 Workflows 区域查看已有工作流
- 点击查看详情，点 Run 执行（输入框可留空直接执行）

**通过 API：**
- 查看所有工作流：GET /api/workflows
- 创建工作流：POST /api/workflows，传 JSON 定义
- AI 生成：POST /api/workflows/generate，传入描述
- 执行：POST /api/workflows/{id}/run，传入输入内容（input_text 可为空字符串）
- 恢复暂停的工作流：POST /api/workflows/{id}/resume，传入 run_id 和 human_input

### 创建工作流

当你需要创建一个工作流时，用 curl 调用 POST /api/workflows：

```bash
curl -X POST http://localhost:8081/api/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "工作流名称",
    "description": "描述",
    "nodes": [...],
    "edges": [...]
  }'
```

### 节点类型

**agent 节点**（默认）— 调用 LLM：
```json
{
  "id": "translate",
  "agent_id": "assistant",
  "name": "翻译",
  "prompt": "将以下内容翻译成英文：\n\n{{input}}",
  "output_var": "translation"
}
```
- agent_id 必须是系统中已有的 agent
- prompt 中用 {{变量名}} 引用前序节点的输出
- 第一个节点可以用 {{input}} 引用用户运行时的输入

**code 节点** — 执行 Python 代码：
```json
{
  "id": "get_date",
  "type": "code",
  "name": "获取日期",
  "code": "import datetime\nresult = datetime.datetime.now().strftime('%m月%d日')",
  "output_var": "today"
}
```
- 不需要 agent_id
- code 字段是 Python 代码字符串
- 代码中可以通过 `variables` dict 读写工作流变量
- 设置 `result = xxx` 控制输出值，否则捕获 stdout
- 可用模块：json, math, re, datetime
- 不可用：import, open, getattr, type（安全限制）

**condition 节点** — LLM 判断分支（通过 conditional_edges 定义）：
```json
{
  "from": "review",
  "condition": "翻译质量是否足够？",
  "paths": {"approved": "__end__", "revise": "translate"}
}
```

**human 节点** — 暂停等人确认：
```json
{
  "id": "confirm",
  "type": "human",
  "prompt": "以上翻译结果是否满意？",
  "output_var": "human_response"
}
```

**parallel 节点** — 并行处理列表中每个元素：
```json
{
  "id": "batch",
  "type": "parallel",
  "agent_id": "assistant",
  "prompt": "处理：{{item}}",
  "items_var": "items_list",
  "item_var": "item",
  "output_var": "results"
}
```

### 边（edges）

边定义节点之间的连接：
```json
{
  "edges": [
    {"from": "__start__", "to": "first_node"},
    {"from": "first_node", "to": "second_node"},
    {"from": "second_node", "to": "__end__"}
  ]
}
```
- __start__ 是工作流入口，__end__ 是出口
- 必须有且只有一条从 __start__ 出发的边
- 必须至少有一条到达 __end__ 的边

### 完整例子

用户说"帮我做一个获取今天日期然后搜索历史事件的工作流"：

```bash
curl -X POST http://localhost:8081/api/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "历史上的今天",
    "description": "获取日期并搜索历史事件",
    "nodes": [
      {
        "id": "get_date",
        "type": "code",
        "name": "获取日期",
        "code": "import datetime\nresult = datetime.datetime.now().strftime(\"%m月%d日\")",
        "output_var": "today"
      },
      {
        "id": "search",
        "agent_id": "assistant",
        "name": "搜索事件",
        "prompt": "搜索{{today}}在历史上发生的重要事件，列出至少5件",
        "output_var": "events"
      }
    ],
    "edges": [
      {"from": "__start__", "to": "get_date"},
      {"from": "get_date", "to": "search"},
      {"from": "search", "to": "__end__"}
    ]
  }'
```

创建工作流后，可以立即执行：
```bash
curl -X POST http://localhost:8081/api/workflows/{id}/run \
  -H "Content-Type: application/json" \
  -d '{"input_text": ""}'
```

### 重要提示

- 创建工作流后，用户可以在前端侧边栏 Workflows 区域看到并点击 Run 执行
- code 节点不需要输入，可以直接留空 input_text
- agent 节点的 prompt 会发给 LLM，所以要写清楚指令
- 变量通过 {{变量名}} 模板替换，code 节点通过 variables dict 访问
