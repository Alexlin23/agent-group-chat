# Agent Group Chat v3 — 人类群聊模式设计方案

## 目标

从"排队处理"升级为"人类群聊"模式：多个agent同时活跃，共享实时对话状态，谁先完成谁先显示，自然交错不阻塞。

## 当前状态（v2）

分支: `feature/langgraph-streaming`
端口: 8081

```
当前架构:
  用户发消息 → 后端lock → 一个task处理整条@mention链 → 逐字SSE推流 → 完成后释放lock
  第二条消息必须等第一条跑完才能开始
```

已实现的组件:
- `event_buffer.py` — 事件缓冲区，支持SSE订阅和断线重放
- `graph.py` — LangGraph状态图，agent编排+@mention链
- `server.py` — FastAPI后台Task模式
- `index.html` — EventSource自动重连

## 人类群聊的关键特征

| 特征 | 当前v2 | 目标v3 |
|------|--------|--------|
| 共享视角 | 各task拿快照隔离 | 所有agent共享实时消息流 |
| 并发发言 | 一次只有一个agent在输出 | 多个agent同时streaming |
| 发言节奏 | @了就必须回复 | agent可以选择性参与 |
| 消息排序 | 严格按task完成顺序 | 按完成时间自然交错 |
| 新消息响应 | 等旧task跑完 | 立即打断@mention链条，开始新task |
| 上下文 | 基于快照（可能过时） | 基于最新消息流 |

## 架构设计

### 核心组件

```
┌─────────────────────────────────────────────────────┐
│                    FastAPI Server                     │
│                                                      │
│  ┌──────────────┐    ┌──────────────────────────┐   │
│  │  Orchestrator │    │     MessageBus           │   │
│  │  (消息路由)    │───→│  (共享对话状态)           │   │
│  │               │    │  - messages: list[dict]   │   │
│  │  接收用户消息  │    │  - subscribe/publish      │   │
│  │  分发给agent  │    │  - 持久化到JSON           │   │
│  └──────┬───────┘    └──────────────────────────┘   │
│         │                    ↑    ↕                  │
│         ↓                    │    │                  │
│  ┌──────────────────────────────────────────────┐   │
│  │              AgentPool (agent池)              │   │
│  │                                               │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐        │   │
│  │  │ Agent   │ │ Agent   │ │ Agent   │ ...    │   │
│  │  │ Worker  │ │ Worker  │ │ Worker  │        │   │
│  │  │ (独立)   │ │ (独立)   │ │ (独立)   │        │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘        │   │
│  │       │           │           │              │   │
│  └───────┼───────────┼───────────┼──────────────┘   │
│          ↓           ↓           ↓                   │
│  ┌──────────────────────────────────────────────┐   │
│  │              EventBuffer (多写者)              │   │
│  │  events: list[dict]                           │   │
│  │  每个event带 task_id + agent_id               │   │
│  └──────────────────────────────────────────────┘   │
│                       ↓                              │
│              GET /stream (SSE)                        │
└─────────────────────────────────────────────────────┘
```

### 1. MessageBus（消息总线）

```python
class MessageBus:
    """共享对话状态，所有agent读同一个消息列表。"""

    messages: list[dict]           # 完整对话历史
    _subscribers: list[asyncio.Event]  # 新消息通知
    _write_lock: asyncio.Lock      # 保护文件写入（不保护读取）

    def get_snapshot(self) -> list[dict]:
        """获取当前消息快照（agent启动时用）。"""
        return list(self.messages)

    def get_latest(self, since: int) -> list[dict]:
        """获取since之后的新消息（实时上下文用）。"""
        return self.messages[since:]

    async def append(self, msg: dict):
        """原子追加消息，通知所有订阅者。"""
        async with self._write_lock:
            self.messages.append(msg)
            self._persist()
        self._notify()

    async def subscribe(self) -> AsyncGenerator[dict, None]:
        """订阅新消息（用于实时上下文增强）。"""
        ...
```

### 2. AgentWorker（独立agent工作单元）

```python
class AgentWorker:
    """一个agent的独立处理单元。不依赖其他worker。"""

    def __init__(self, agent_id, message_bus, event_buffer, task_id):
        self.agent_id = agent_id
        self.bus = message_bus
        self.events = event_buffer
        self.task_id = task_id
        self._cancelled = False

    async def run(self):
        """执行agent处理：读上下文 → 调Hermes → 写回消息。"""
        # 1. 读取最新消息快照
        snapshot = self.bus.get_snapshot()

        # 2. 构建system prompt
        prompt = self._build_prompt(snapshot)

        # 3. 调Hermes API，逐字推EventBuffer
        self.events.push("agent_start", {
            "agent_id": self.agent_id,
            "task_id": self.task_id,
        })
        response = ""
        async for chunk in stream_hermes(prompt):
            if self._cancelled:
                break
            response += chunk
            self.events.push("text", {
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "text": chunk,
            })

        # 4. 写回消息总线
        if response and not self._cancelled:
            await self.bus.append({
                "role": "assistant",
                "content": clean(response),
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "timestamp": datetime.now().isoformat(),
            })
            self.events.push("agent_done", {
                "agent_id": self.agent_id,
                "task_id": self.task_id,
            })

    def cancel(self):
        """取消当前agent（中断@mention链条用）。"""
        self._cancelled = True
```

### 3. Orchestrator（编排器）

```python
class Orchestrator:
    """管理agent生命周期，决定谁参与、何时参与。"""

    MAX_CONCURRENT = 3  # 最多同时3个agent在发言（模拟人类群聊节奏）

    async def handle_user_message(self, conv_id, content, targets):
        """处理用户消息：分发给相关agent。"""

        # 1. 写入MessageBus
        await self.bus.append({"role": "user", "content": content, ...})

        # 2. 确定参与的agent
        if not targets:
            targets = list(self.agents.keys())

        # 3. 并发启动agent workers（受MAX_CONCURRENT限制）
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)

        async def run_with_limit(worker):
            async with semaphore:
                await worker.run()

        workers = [
            AgentWorker(tid, self.bus, self.event_buffer, task_id=f"{conv_id}_{i}")
            for i, tid in enumerate(targets)
        ]
        await asyncio.gather(*[run_with_limit(w) for w in workers])

        # 4. 处理@mention链条（被@的agent自动加入）
        # 这部分在workers完成后再做，或者在worker.run()内部做
```

### 4. 前端多流显示

```javascript
// 状态：支持多个并发streaming
state.streamingAgents = new Map();  // key: `${taskId}_${agentId}`, value: {response, element}

// agent_start → 创建新的streaming气泡（多个可以并存）
// text → 更新对应agent的气泡
// agent_done → 完成对应agent的气泡

// 消息显示：按timestamp排序，自然交错
```

## 实现计划

### Phase 1: MessageBus + 多写者EventBuffer（后端基础）

**改动文件:** `server.py`, `event_buffer.py`
**工作量:** ~2小时

- [ ] 提取MessageBus类（从server.py的conversations存储中独立出来）
- [ ] EventBuffer支持多写者（当前已支持，确认无竞争）
- [ ] 消息持久化从task内部移到MessageBus.append()
- [ ] 每个消息/事件带task_id标识

**验证:** 手动模拟两个并发写入，确认消息不丢失

### Phase 2: AgentWorker独立化（核心改造）

**改动文件:** `graph.py`（重写为AgentWorker）
**工作量:** ~3小时

- [ ] 将LangGraph的process_agent节点改为独立的AgentWorker类
- [ ] 每个worker独立运行，不依赖graph的状态传递
- [ ] worker从MessageBus读快照，写回MessageBus
- [ ] 支持cancel（中断@mention链条）

**验证:** 手动启动两个worker并发，确认各自streaming正常

### Phase 3: Orchestrator并发调度

**改动文件:** `server.py`
**工作量:** ~2小时

- [ ] POST /message → Orchestrator.handle_user_message()
- [ ] 用asyncio.Semaphore限制并发数（MAX_CONCURRENT=3）
- [ ] @mention链条：worker完成后检查回复中的@mention，spawn新worker
- [ ] 中断逻辑：新消息进来时，cancel旧task的@mention链条

**验证:** 发两条消息，确认第二条不等第一条跑完

### Phase 4: 前端多流UI

**改动文件:** `index.html`
**工作量:** ~3小时

- [ ] state.streamingAgents改为Map，支持多个并发
- [ ] agent_start创建独立气泡（带task_id标记）
- [ ] text事件更新对应气泡（按task_id+agent_id查找）
- [ ] agent_done完成对应气泡
- [ ] 多个"正在输入"指示器并排显示
- [ ] 消息按timestamp排序渲染

**验证:** 两个agent同时streaming，UI正确显示两组气泡

### Phase 5: 自然节奏（高级）

**工作量:** ~2小时

- [ ] agent"阅读延迟"：启动时随机等待0.5-2秒再开始回复（模拟阅读时间）
- [ ] 选择性参与：agent判断消息是否跟自己相关，不相关就不回复
- [ ] 中断友好：新消息进来时，当前agent完成当前token后停止，不继续@mention链

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 消息顺序错乱 | 低——前端按timestamp排序即可 | 自然交错，不需要严格排序 |
| 上下文过时 | 中——agent基于快照可能漏看新消息 | Phase 1先用快照，Phase 5再做实时 |
| API并发压力 | 中——多个agent同时调Hermes | Semaphore限制并发数 |
| 前端复杂度 | 高——多流状态管理比单流复杂很多 | Phase 4单独做，充分测试 |
| JSON文件并发写入 | 低——asyncio单线程，append原子 | write_lock保护文件写入 |

## 开放问题

1. **MAX_CONCURRENT设多少？** 3是初始建议，可以根据实际体验调整
2. **@mention链条深度？** 当前是1000，群聊模式下建议限制为2-3层，避免无限循环
3. **agent回复中的@mention怎么处理？** 是立即spawn新worker还是等当前group完成？
4. **前端如何区分不同task的消息？** 用颜色？用分隔线？还是自然混排？

## 文件结构（改造后）

```
agent-group-chat/
├── event_buffer.py    # 不变
├── message_bus.py     # 新增：共享消息状态
├── agent_worker.py    # 新增：独立agent工作单元
├── orchestrator.py    # 新增：并发调度
├── server.py          # 改造：用Orchestrator替代直接task
├── index.html         # 改造：多流UI
├── agents.yaml        # 不变
└── conversations/     # 不变
```
