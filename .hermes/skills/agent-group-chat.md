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

## 三、Task Flow 工作流

Task Flow 是多 Agent 流水线，按步骤顺序执行，前一步的输出可以传给下一步。

**通过前端：**
- 侧边栏 Task Flows 区域查看已有工作流
- 点击查看详情，点 Run 执行
- 可以用自然语言描述需求，AI 自动生成工作流结构

**通过 API：**
- 查看所有工作流：GET /api/task-flows
- 创建工作流：POST /api/task-flows
- AI 生成：POST /api/task-flows/generate，传入描述
- 执行：POST /api/task-flows/{id}/run，传入输入内容

**工作流定义：**
- 每个工作流有多个步骤（steps）
- 每步指定一个 Agent、一个提示词模板、一个输出变量名
- 模板中用 {{变量名}} 引用前序步骤的输出
- 用户运行时输入的内容通过 {{input}} 传给第一步
