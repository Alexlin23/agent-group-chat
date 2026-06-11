"""Workflow API routes — LangGraph-powered workflow engine."""

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from event_buffer import EventBuffer, format_sse
from workflow_engine import execute_workflow, resume_workflow

router = APIRouter(prefix="/api/workflows", tags=["workflows"])


def _state():
    import sys
    return sys.modules['__main__']


# ── Request/Response Models ──

class WorkflowCreate(BaseModel):
    name: str
    description: str = ""
    nodes: list[dict] = []
    edges: list[dict] = []
    conditional_edges: list[dict] = []


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    nodes: Optional[list[dict]] = None
    edges: Optional[list[dict]] = None
    conditional_edges: Optional[list[dict]] = None


class WorkflowGenerateRequest(BaseModel):
    description: str


class WorkflowRunRequest(BaseModel):
    input_text: str


class WorkflowResumeRequest(BaseModel):
    run_id: str
    human_input: str


# ── CRUD ──

@router.get("")
async def list_workflows():
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    return s.workflow_manager.list_workflows()


@router.post("")
async def create_workflow(req: WorkflowCreate):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    try:
        return s.workflow_manager.create_workflow(
            name=req.name, description=req.description,
            nodes=req.nodes, edges=req.edges,
            conditional_edges=req.conditional_edges,
        )
    except ValueError as e:
        raise HTTPException(422, str(e))


@router.post("/generate")
async def generate_workflow(req: WorkflowGenerateRequest):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    try:
        return await s.workflow_manager.generate_workflow(req.description)
    except Exception as e:
        raise HTTPException(422, f"Generation failed: {str(e)[:300]}")


@router.get("/{wf_id}")
async def get_workflow(wf_id: str):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    wf = s.workflow_manager.get_workflow(wf_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")
    return wf


@router.put("/{wf_id}")
async def update_workflow(wf_id: str, req: WorkflowUpdate):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    try:
        wf = s.workflow_manager.update_workflow(
            wf_id, name=req.name, description=req.description,
            nodes=req.nodes, edges=req.edges,
            conditional_edges=req.conditional_edges,
        )
    except ValueError as e:
        raise HTTPException(422, str(e))
    if not wf:
        raise HTTPException(404, "Workflow not found")
    return wf


@router.delete("/{wf_id}")
async def delete_workflow(wf_id: str):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    if not s.workflow_manager.delete_workflow(wf_id):
        raise HTTPException(404, "Workflow not found")
    return {"ok": True}


# ── Execution ──

@router.post("/{wf_id}/run")
async def run_workflow(wf_id: str, req: WorkflowRunRequest):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")

    wf = s.workflow_manager.get_workflow(wf_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")

    buffer = EventBuffer()
    run = s.workflow_manager.create_run(wf_id, req.input_text)
    run_id = run["id"]

    async def _run():
        result = None
        try:
            result = await execute_workflow(
                workflow_def=wf,
                input_text=req.input_text,
                agents=s.app_state.agents,
                hermes_url=s.HERMES_API_URL,
                hermes_key=s.HERMES_API_KEY,
                event_buffer=buffer,
                run_id=run_id,
            )
        except Exception as e:
            # execute_workflow raised — update run to failed, close buffer
            s.workflow_manager.update_run(run_id, status="failed", error=str(e)[:300])
            if buffer.is_active:
                buffer.push("error", {"error": str(e)[:300]})
                buffer.close()
            return
        finally:
            print(f'[_run] END run_id={run_id}', file=sys.stderr, flush=True)
            s.active_buffers.pop(f"wf_{run_id}", None)

        # execute_workflow returned — result is always a dict here
        status = result.get("status", "completed")
        if status == "paused":
            s.workflow_manager.update_run(run_id, status="paused")
        else:
            s.workflow_manager.update_run(
                run_id, status=status,
                error=result.get("error", ""),
                variables=result.get("variables", {}),
                execution_log=result.get("execution_log", []),
            )

    s.active_buffers[f"wf_{run_id}"] = buffer
    task = asyncio.create_task(_run())
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    return {"status": "accepted", "run_id": run_id, "workflow_id": wf_id}


@router.post("/{wf_id}/resume")
async def resume_workflow_endpoint(wf_id: str, req: WorkflowResumeRequest):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")

    wf = s.workflow_manager.get_workflow(wf_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")

    buffer = EventBuffer()

    async def _resume():
        result = None
        try:
            result = await resume_workflow(
                workflow_def=wf,
                run_id=req.run_id,
                human_input=req.human_input,
                agents=s.app_state.agents,
                hermes_url=s.HERMES_API_URL,
                hermes_key=s.HERMES_API_KEY,
                event_buffer=buffer,
            )
        except Exception as e:
            s.workflow_manager.update_run(req.run_id, status="failed", error=str(e)[:300])
            if buffer.is_active:
                buffer.push("error", {"error": str(e)[:300]})
                buffer.close()
            return
        finally:
            s.active_buffers.pop(f"wf_{req.run_id}", None)

        status = result.get("status", "completed")
        if status == "paused":
            s.workflow_manager.update_run(req.run_id, status="paused")
        else:
            s.workflow_manager.update_run(req.run_id, status=status)

    s.active_buffers[f"wf_{req.run_id}"] = buffer
    asyncio.create_task(_resume())
    return {"status": "accepted", "run_id": req.run_id, "workflow_id": wf_id}


# ── Runs ──

@router.get("/{wf_id}/runs")
async def list_runs(wf_id: str):
    s = _state()
    if s.workflow_manager is None:
        raise HTTPException(503, "Workflow manager not initialized")
    return s.workflow_manager.list_runs(wf_id)


@router.get("/runs/{run_id}/stream")
async def stream_run(run_id: str, last_id: int = Query(-1, alias="last_id")):
    s = _state()
    buffer = s.active_buffers.get(f"wf_{run_id}")

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
