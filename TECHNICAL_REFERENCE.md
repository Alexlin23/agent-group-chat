# Agent Group Chat 技术参考

从 hermes-multi-agent-chat 和 realtime-multi-agent-chat 两个skill中提炼的、与本项目直接相关的内容。

---

## 1. 流式架构：Background Task + EventBuffer + EventSource

### 问题

传统SSE流式把agent处理绑定到HTTP连接上。用户刷新页面 → 连接断开 → generator取消 → 进行中的工作全部丢失。

### 三层解耦方案

```
┌─────────────────────────────────────────┐
│  Background Task (asyncio.Task)          │
│  - 独立运行agent处理                      │
│  - 推送事件到EventBuffer                  │
│  - HTTP连接断开不影响运行                  │
└──────────────┬──────────────────────────┘
               ↓ events
┌─────────────────────────────────────────┐
│  EventBuffer (内存事件日志)               │
│  - 顺序递增event ID                       │
│  - 异步订阅 + 回放支持                     │
│  - 30秒空闲心跳                           │
│  - close通知订阅者停止                     │
└──────────────┬──────────────────────────┘
               ↓ SSE
┌─────────────────────────────────────────┐
│  EventSource (浏览器)                     │
│  - 自动重连                               │
│  - Last-Event-Id回放                      │
│  - GET方式（POST走普通fetch）              │
└─────────────────────────────────────────┘
```

### 数据流

1. 用户发消息 → POST /message → 启动asyncio.Task → 立即返回
2. 后台任务处理agents，推送事件到EventBuffer
3. 前端连接 GET /stream → EventSource实时接收事件
4. 刷新页面 → EventSource自动重连 → 服务端从buffer回放 → 无丢失
5. 任务完成 → buffer关闭 → "done"事件 → 前端结束

### 关键设计选择

**POST立即返回（fire-and-forget）**
- 处理期间不保持HTTP连接
- 前端收到 `{status: "accepted"}` 后连接独立的SSE端点
- 关注点分离：POST输入，GET输出

**EventBuffer顺序ID**
- 每个事件单调递增ID (0, 1, 2, ...)
- SSE格式：`id: N\nevent: type\ndata: {...}\n\n`
- 浏览器EventSource重连时发送 `Last-Event-Id: N`
- 服务端从 `last_id + 1` 开始回放

**EventSource优于fetch+ReadableStream**
- 自动重连（浏览器内置，无需自定义代码）
- GET-only（POST用普通fetch）
- 原生 `Last-Event-Id` 头支持
- 前端代码更简洁

**空闲心跳**
- 30秒无新事件 → 发送心跳
- 防止代理/负载均衡器断开空闲连接
- 心跳事件被 format_sse() 过滤（不发给客户端）

### 竞态条件陷阱

| 场景 | 修复 |
|------|------|
| 任务在前端连接前完成 | `no_task`处理器必须重置所有流式状态 |
| 旧EventSource阻塞新连接 | 创建新连接前先关闭旧的 |
| 刷新时多个连接共存 | 前端显式关闭旧连接 |

---

## 2. Hermes API Server 关键行为

来源：gateway/platforms/api_server.py

### 认证
- `Authorization: Bearer <key>` 对比 `API_SERVER_KEY`
- 无key配置时允许所有请求（仅限本地）
- 使用 `hmac.compare_digest` 防时序攻击

### Session ID推导（无 X-Hermes-Session-Id 时）
```python
session_id = _derive_chat_session_id(system_prompt, first_user)
```
- 确定性：相同system_prompt + 相同first_user = 相同session_id
- 历史在state.db中跨请求累积
- **这是P1（历史累积bug）的根因**

### 消息处理
```python
# System消息 → 拼接为system_prompt
# User/assistant消息 → conversation_messages
# 最后一条user → 主输入(user_message)
# 之前的所有 → 历史(history)
user_message = conversation_messages[-1]
history = conversation_messages[:-1]
```

### Session续接（X-Hermes-Session-Id）
- 必须设置API_SERVER_KEY（否则403）
- 提供时：从state.db加载历史，忽略请求体中的历史

### 流式
- 请求体 `stream: true`
- 返回SSE格式
- 每个chunk：`data: {"choices": [{"delta": {"content": "..."}}]}\n\n`
- 结束：`data: [DONE]\n\n`

### 端口/CORS
- 默认：127.0.0.1:8642
- API_SERVER_CORS_ORIGINS：逗号分隔的允许源列表

---

## 3. 内存管理配置

### 配置块
```python
TOKENIZER_BACKEND = os.getenv("TOKENIZER_BACKEND", "tiktoken")  # "tiktoken" or "char_estimate"
CONTEXT_WINDOW_BUDGET_RATIO = float(os.getenv("CONTEXT_WINDOW_BUDGET_RATIO", "0.7"))
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "20"))  # 轮次，非消息数
SUMMARY_TRIGGER_INTERVAL = int(os.getenv("SUMMARY_TRIGGER_INTERVAL", "10"))  # 消息数
FACT_STALE_THRESHOLD = int(os.getenv("FACT_STALE_THRESHOLD", "30"))  # 距上次提及的消息数
```

### Token计数（CJK友好）
```python
_tiktoken_enc = None
if TOKENIZER_BACKEND == "tiktoken":
    try:
        import tiktoken
        _tiktoken_enc = tiktoken.get_encoding("cl100k_base")
    except ImportError:
        TOKENIZER_BACKEND = "char_estimate"

def count_tokens(text: str) -> int:
    if _tiktoken_enc is not None:
        return len(_tiktoken_enc.encode(text))
    cjk = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    ascii_chars = len(text) - cjk
    return cjk * 2 + max(1, ascii_chars // 4)
```

### Token计数精度对比

| 后端 | "Hello world" | "你好世界" | "编码者收到，审查者确认后开工" |
|------|--------------|-----------|---------------------------|
| tiktoken cl100k_base | 2 | 5 | 13 |
| char_estimate fallback | 2 | 9 | 27 |

CJK估算比tiktoken高约1.5-2倍。优先用tiktoken。

### 摘要对象格式
```json
{
  "text": "用户提出了三层记忆架构的需求...",
  "msg_range": [0, 25],
  "token_count": 150,
  "created_at": "2026-06-04T10:30:00"
}
```

### 事实对象格式
```json
{
  "text": "项目使用FastAPI+Hermes API Server架构，端口8080",
  "token_count": 25,
  "last_mentioned_at": "2026-06-04T10:30:00",
  "mentions": 3
}
```

### 过期检测
`current_msg_index - fact.last_mentioned_msg_index > FACT_STALE_THRESHOLD` 时标记过期，不注入L3上下文，再次提及时复活。

---

## 4. 核心组件模式

### MessageBus（共享对话状态）
所有agent读同一份消息列表。写操作原子化（asyncio.Lock只保护文件I/O，不保护读）。

```python
class MessageBus:
    messages: list[dict]
    _write_lock: asyncio.Lock  # 只保护文件I/O

    def get_snapshot(self) -> list[dict]:  # 给agent worker的副本
    async def append(self, msg: dict) -> int:  # 原子写+持久化
    async def append_batch(self, msgs: list[dict]) -> int:
```

**关键坑**：`__init__`中必须从已有JSON文件初始化`_name`和`_created_at`。不要在`_persist()`中用`hasattr`检查——会崩。

### AgentWorker（独立处理）
每个agent作为独立async任务运行。从MessageBus快照读取，流式写入EventBuffer，完成后回写。

```python
class AgentWorker:
    async def run(self) -> dict | None:  # 返回新消息或None
    def cancel(self):  # 设置_cancelled标志，每个streaming chunk检查
```

**关键坑**：cancel只在streaming chunks之间生效。如果agent在等下一个chunk，要等那个chunk到了才检查`_cancelled`。

### Orchestrator（并发调度）
管理agent生命周期。用`asyncio.Semaphore(N)`限制并发（N=3推荐，节奏像人）。

```python
class Orchestrator:
    async def process_message(self, conv_id, user_message, target_ids, bus, event_buffer):
        # 1. 取消该对话的旧链
        # 2. 并发启动target agents的workers
        # 3. 处理@mention链（按深度顺序）
    def cancel_chain(self, conv_id):  # 中断运行中的workers
    def cut_mention_chain(self, conv_id):  # 让workers完成当前回复，停止@mention传播
```

**关键坑**：取消旧链时，旧workers的`finally`块不能清理新workers拥有的资源。用身份检查：`if active_buffers.get(conv_id) is my_buffer`。

### EventBuffer（SSE + 回放）
每个对话一个EventBuffer。支持多个并发写入者（asyncio单线程，append原子）。

```python
class EventBuffer:
    def push(self, event_type: str, data: dict) -> int:  # 返回event ID
    async def subscribe(self, last_id: int = -1):  # 从last_id+1开始yield
    def close(self):  # 通知完成
```

事件包含`task_id`字段区分并发流。

**Buffer策略：per-task vs shared**
- Per-task（生产选择）：每个任务独立buffer，生命周期简单。缺点：用户发新消息时前端断开旧流，旧任务的流式事件丢失（最终持久化结果不丢）。
- Shared（尝试过但放弃）：所有任务写同一buffer。缺点：不同任务的事件交错，UX混乱。

### Hermes API调用模式
项目用`hermes_client.py`封装两种调用：
- `stream_hermes()`: SSE流式，yield内容chunks
- `call_hermes()`: 一次性请求，返回完整响应

关键：system_prompt中嵌入完整对话历史，user_message固定为"请回复。"。这样每次请求自包含，避免session历史累积。

---

## 5. Pitfall速查表

| Pitfall | 症状 | 修复 |
|---------|------|------|
| `_name`未初始化 | 首次persist时AttributeError | `__init__`中从JSON加载元数据 |
| 旧task的finally清理新buffer | 中断后新消息消失 | 身份检查：`if active_buffers.get(conv_id) is my_buffer` |
| 前端排队消息 | 新消息等旧链完成才发 | 断开旧流，立即发送 |
| `no_task`不重置`isStreaming` | 后续发送卡住 | `no_task`处理器调用`onStreamEnd()` |
| 旧EventSource阻塞新连接 | 流连不上 | 创建新ES前先关闭旧的 |
| 发送后无即时反馈 | 用户多次点击发送 | POST返回前显示占位符 |
| `cut_mention_chain` vs `cancel_chain` | 用户发新消息时旧task被杀 | 新消息用`cut_mention_chain`，删除用`cancel_chain` |
| Per-task EventBuffer | 旧task流式丢失 | 接受tradeoff或多连接方案 |
| 无`last_id`重连跟踪 | 重复事件→重复UI元素 | 跟踪`lastEventId`，重连时传`?last_id=N` |
| `disconnectStream`清空流式状态 | 切换对话时流中断 | 只关EventSource，不清`streamingAgents` |
| 全局`isStreaming`标志 | 一个对话流式时所有对话输入锁死 | 用per-conversation状态 |
| 重复`const`声明 | JS静默失败，整个前端停 | 同作用域加变量前先检查 |
| 缓存DOM元素引用 | `renderMessages()`后引用失效 | 用`querySelector`获取新元素 |
| prompt缺时间戳 | agent无法判断节奏，总是立即回复 | 用相对时间（刚刚/X分钟前） |
| prompt缺群聊上下文 | agent不知道在多人环境，过度回复 | 注入"多人聊天环境"块+参与规则 |
| 改skill文档不改代码 | 用户要求改行为，你改了SKILL.md | 先定位项目文件，直接改代码 |
| 共享EventBuffer事件交错 | 流式停了只输出最终结果 | 用per-task buffer |
| CSS颜色标准化 | JS比较`#4FC3F7`和`rgb(79,195,247)`永远不等 | 用`data-*`属性存原始值 |
| 浏览器缓存静态文件 | 改了index.html刷新不生效 | 硬刷新(Ctrl+Shift+R)或加`Cache-Control`头 |

---

## 6. Agent行为调优

### 讨论后再委派
架构师prompt："先跟用户讨论清楚，等用户确认后再@编码者。不要分析完就直接甩给编码者。"

### 选择性参与
用户没@任何人时，只触发第一个agent。其他通过@mention链加入。

```python
if not target_ids:
    target_ids = [agent_list[0]["id"]]  # 不是全部agent
```

### 每个agent的参与规则
"用户没@你时，先判断消息是否跟自己相关，不相关就不回复"

### 不要让agent问用户"要不要做"
agent应通过prompt规则自主判断，不是每步都问用户。反面案例："B太傻了，他们聊天问我干嘛"。

### 工具访问控制
如果agent有文件访问权限（通过Hermes tools），讨论代码改进时可能实际修改文件。群聊用的agent profile不要给terminal/file权限，或用独立无工具profile。

---

## 7. systemd服务配置

```ini
# ~/.config/systemd/user/agent-group-chat.service
[Unit]
Description=Agent Group Chat Server
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/project
Environment=HERMES_API_KEY=<key>
Environment=CHAT_SERVER_PORT=8081
ExecStart=/path/to/.venv/bin/python server.py
Restart=on-failure
RestartSec=5
KillMode=control-group
TimeoutStopSec=5
```

**KillMode=control-group是关键**。不用它的话，`systemctl restart`可能留旧Python进程活着（占端口），新进程报`EADDRINUSE`。

```bash
systemctl --user daemon-reload
systemctl --user enable agent-group-chat
systemctl --user start agent-group-chat
```
