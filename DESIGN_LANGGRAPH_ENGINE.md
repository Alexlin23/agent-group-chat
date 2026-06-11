# LangGraph Workflow Engine — 模块化设计方案

## 目标
用真正的 LangGraph StateGraph 替代现有线性 for 循环，支持：
- 顺序执行
- 条件分支
- 并行步骤（fan-out / fan-in）
- 循环（重复执行直到条件满足）
- 检查点（断点恢复）
- 人类介入（暂停等人确认）
- AI 自动生成工作流

## 模块划分

### Module 1: workflow_state.py — 状态定义
职责：定义 TypedDict 状态结构，所有模块共享
可独立测试：✅ 直接实例化验证
验收标准：能创建状态实例，字段类型正确

```python
class WorkflowState(TypedDict):
    workflow_id: str
    current_node: str
    variables: dict[str, str]      # 步骤间传递的变量
    messages: Annotated[list[dict], operator.add]  # 对话历史
    node_outputs: dict[str, str]   # 每个节点的输出
    execution_log: Annotated[list[dict], operator.add]  # 执行日志
    status: str                    # running / paused / completed / failed
    human_input: str               # 人类介入时的输入
    event_buffer: Any              # SSE 事件缓冲
    agents_ref: dict[str, dict]    # agent 定义引用
    hermes_url: str
    hermes_key: str
```

### Module 2: workflow_nodes.py — 节点函数
职责：定义所有节点类型的执行逻辑
- agent_node: 调用 agent 执行任务
- condition_node: LLM 判断分支走向
- parallel_node: fan-out 多个子任务
- human_node: 暂停等待人类输入
- merge_node: fan-in 汇总并行结果
可独立测试：✅ mock hermes_client 验证每种节点
验收标准：每种节点接收 state 返回 state update

### Module 3: workflow_graph.py — 图构建器
职责：根据 workflow 定义（JSON/YAML）构建 LangGraph StateGraph
- 解析 nodes 和 edges 定义
- 添加节点、条件边、并行分支
- 编译图
可独立测试：✅ 构建图后 graph.nodes 有正确节点
验收标准：构建出的图结构匹配定义

### Module 4: workflow_engine.py — 执行引擎
职责：运行编译好的图
- invoke / ainvoke 调用
- 检查点管理（MemorySaver）
- 人类介入恢复（Command(resume=...)）
- SSE 事件推送
可独立测试：✅ 传入 mock state 运行
验收标准：能跑完一个简单顺序流程

### Module 5: workflow.py — CRUD 管理器（替换 task_flow.py）
职责：工作流的创建/读取/更新/删除
- create_workflow / list / get / update / delete
- generate_workflow（AI 生成）
- create_run / update_run / list_runs
可独立测试：✅ CRUD 操作不依赖引擎
验收标准：创建→读取→更新→删除 全流程通过

### Module 6: routes/workflows.py — API 路由（替换 routes/task_flows.py）
职责：REST API 端点
可独立测试：✅ httpx TestClient 测试
验收标准：每个端点返回正确状态码和数据

### Module 7: workflow_serializer.py — 序列化/反序列化
职责：workflow 定义的 JSON 存储和加载
- 序列化：Python dict → JSON 文件
- 反序列化：JSON → workflow 定义 dict
- 验证：检查必填字段、agent_id 存在性
可独立测试：✅ 纯数据转换
验收标准：序列化→反序列化→对比一致

## 执行顺序

Phase 1（基础框架，不破坏现有功能）:
1. Module 1: workflow_state.py — 状态定义
2. Module 7: workflow_serializer.py — 存储格式
3. Module 5: workflow.py — CRUD 管理器
→ 验收：能创建/读取/更新/删除 workflow 定义

Phase 2（引擎核心）:
4. Module 2: workflow_nodes.py — 节点函数
5. Module 3: workflow_graph.py — 图构建器
6. Module 4: workflow_engine.py — 执行引擎
→ 验收：能执行一个 3 步顺序 workflow

Phase 3（高级功能）:
7. 条件分支
8. 并行执行
9. 循环
10. 人类介入
11. 检查点
→ 验收：每种功能独立测试通过

Phase 4（集成）:
12. Module 6: routes/workflows.py — API 路由
13. 前端适配（index.html）
14. 删除旧 task_flow 相关代码
→ 验收：端到端通过 API 创建→执行→查看结果

## 与现有代码的关系

保留：
- graph.py — 继续做群聊 @mention 链（不动）
- orchestrator.py — 群聊调度（不动）
- agent_worker.py — 单 agent 处理（不动）

替换：
- task_flow.py → workflow.py
- workflow_engine.py → 重写为 LangGraph
- routes/task_flows.py → routes/workflows.py
- task_flows/ 目录 → workflows/
- task_flow_runs/ 目录 → workflow_runs/

## 关键设计决策

1. graph.py 不动 — 它负责群聊 @mention 链，和 workflow 引擎是两个独立系统
2. workflow_graph.py 根据 JSON 定义动态构建图 — 不是写死的
3. 节点函数是独立的 — 每种节点类型一个函数，方便扩展
4. 状态用 TypedDict — LangGraph 标准做法
5. 检查点用 MemorySaver — 先用内存版，后续可换 SQLite
