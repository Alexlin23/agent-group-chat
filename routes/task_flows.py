"""Task Flow API routes."""

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from event_buffer import EventBuffer, format_sse
from models import TaskFlowCreate, TaskFlowUpdate, TaskFlowGenerateRequest, TaskFlowRunRequest

router = APIRouter(prefix="/api/task-flows", tags=["task-flows"])


def _state():
    import server
    return server


@router.get("")
async def list_flows():
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    return s.task_flow_manager.list_flows()


@router.post("")
async def create_flow(req: TaskFlowCreate):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    steps = [step.model_dump() for step in req.steps]
    return s.task_flow_manager.create_flow(req.name, req.description, steps)


@router.post("/generate")
async def generate_flow(req: TaskFlowGenerateRequest):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    try:
        result = await s.task_flow_manager.generate_flow(req.description)
        return result
    except Exception as e:
        raise HTTPException(422, f"Flow generation failed: {str(e)[:300]}")


@router.get("/{flow_id}")
async def get_flow(flow_id: str):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    flow = s.task_flow_manager.get_flow(flow_id)
    if not flow:
        raise HTTPException(404, "Flow not found")
    return flow


@router.put("/{flow_id}")
async def update_flow(flow_id: str, req: TaskFlowUpdate):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    steps = [step.model_dump() for step in req.steps] if req.steps is not None else None
    flow = s.task_flow_manager.update_flow(flow_id, name=req.name, description=req.description, steps=steps)
    if not flow:
        raise HTTPException(404, "Flow not found")
    return flow


@router.delete("/{flow_id}")
async def delete_flow(flow_id: str):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    if not s.task_flow_manager.delete_flow(flow_id):
        raise HTTPException(404, "Flow not found")
    return {"ok": True}


@router.post("/{flow_id}/run")
async def run_flow(flow_id: str, req: TaskFlowRunRequest):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")

    flow = s.task_flow_manager.get_flow(flow_id)
    if not flow:
        raise HTTPException(404, "Flow not found")

    buffer = EventBuffer()
    run = s.task_flow_manager.create_run(flow_id, req.input_text)
    run_id = run["id"]

    async def _run():
        try:
            await s.task_flow_manager.execute(flow_id, req.input_text, buffer)
        except Exception as e:
            buffer.push("error", {"error": str(e)[:300]})
            buffer.close()

    asyncio.create_task(_run())
    return {"status": "accepted", "run_id": run_id, "flow_id": flow_id}


@router.get("/{flow_id}/runs")
async def list_runs(flow_id: str):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")
    return s.task_flow_manager.list_runs(flow_id)


@router.get("/runs/{run_id}/stream")
async def stream_run(run_id: str, last_id: int = Query(-1, alias="last_id")):
    s = _state()
    if s.task_flow_manager is None:
        raise HTTPException(503, "Task Flow manager not initialized")

    buffer = s.task_flow_manager.get_run_buffer(run_id)

    async def event_generator():
        if buffer is None or not buffer.is_active:
            yield format_sse({"type": "no_task", "id": -1})
            return
        async for event in buffer.subscribe(last_id):
            sse = format_sse(event)
            if sse:
                yield sse

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
