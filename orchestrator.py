"""Orchestrator — concurrent agent scheduling.

Two execution modes:
  - "chat": simple AgentWorker (conversational, natural rhythm)
  - "workflow": LangGraph state graph (rigorous pipeline, strict ordering)

Agents default to "chat" mode. Specific agents can be configured as
"workflow" agents for tasks that need严谨性 (code review, multi-step verification).
"""

import asyncio
import uuid
from datetime import datetime
from typing import Optional

from agent_worker import AgentWorker, MAX_RESPONSES_PER_AGENT
from event_buffer import EventBuffer
from message_bus import MessageBus


class Orchestrator:
    """Manages concurrent agent processing for a conversation."""

    def __init__(
        self,
        agents: dict,
        hermes_url: str,
        hermes_key: str,
        max_concurrent: int = 3,
        workflow_agents: set[str] = None,
    ):
        self.agents = agents
        self.hermes_url = hermes_url
        self.hermes_key = hermes_key
        self.max_concurrent = max_concurrent
        self.workflow_agents = workflow_agents or set()  # agent IDs that use LangGraph
        self._active_workers: dict[str, list[AgentWorker]] = {}
        self._cancel_flags: dict[str, bool] = {}

    async def _run_chat_agent(
        self, agent_id: str, bus: MessageBus, event_buffer: EventBuffer,
        task_id: str, depth: int, semaphore: asyncio.Semaphore,
    ) -> Optional[dict]:
        """Run an agent in chat mode (simple AgentWorker)."""
        worker = AgentWorker(
            agent_id=agent_id,
            agents=self.agents,
            message_snapshot=bus.get_snapshot(),
            event_buffer=event_buffer,
            hermes_url=self.hermes_url,
            hermes_key=self.hermes_key,
            task_id=task_id,
            depth=depth,
        )
        async with semaphore:
            return await worker.run()

    async def _run_workflow_agent(
        self, agent_id: str, bus: MessageBus, event_buffer: EventBuffer,
        task_id: str, semaphore: asyncio.Semaphore,
    ) -> list[dict]:
        """Run an agent in workflow mode (LangGraph state graph)."""
        from graph import run_agent_task

        async with semaphore:
            new_messages = await run_agent_task(
                conv_id=task_id,
                user_message="",
                target_ids=[agent_id],
                agents=self.agents,
                existing_messages=bus.get_snapshot(),
                hermes_url=self.hermes_url,
                hermes_key=self.hermes_key,
                event_buffer=event_buffer,
            )
            return new_messages or []

    async def process_message(
        self,
        conv_id: str,
        user_message: str,
        target_ids: list[str],
        bus: MessageBus,
        event_buffer: EventBuffer,
    ):
        """Process a user message: spawn workers for target agents, handle @mentions."""
        run_id = uuid.uuid4().hex[:8]
        semaphore = asyncio.Semaphore(self.max_concurrent)
        response_count: dict[str, int] = {}

        # Cut old @mention chain (let current workers finish, don't spawn new ones)
        self.cut_mention_chain(conv_id)
        self._cancel_flags[conv_id] = False
        self._active_workers[conv_id] = []

        async def run_worker(agent_id: str, depth: int) -> Optional[dict]:
            """Run a single agent worker with semaphore limiting."""
            if self._cancel_flags.get(conv_id, False):
                return None
            if response_count.get(agent_id, 0) >= MAX_RESPONSES_PER_AGENT:
                return None
            if agent_id not in self.agents:
                return None

            response_count[agent_id] = response_count.get(agent_id, 0) + 1
            task_id = f"{run_id}_{agent_id}_{depth}"

            # Dispatch based on agent mode
            if agent_id in self.workflow_agents:
                # Workflow mode: LangGraph (rigorous pipeline)
                new_msgs = await self._run_workflow_agent(
                    agent_id, bus, event_buffer, task_id, semaphore
                )
                return new_msgs  # returns list of messages
            else:
                # Chat mode: simple AgentWorker
                worker = AgentWorker(
                    agent_id=agent_id,
                    agents=self.agents,
                    message_snapshot=bus.get_snapshot(),
                    event_buffer=event_buffer,
                    hermes_url=self.hermes_url,
                    hermes_key=self.hermes_key,
                    task_id=task_id,
                    depth=depth,
                )
                self._active_workers[conv_id].append(worker)

                async with semaphore:
                    msg = await worker.run()

                return msg

        # Phase 1: Process initial target agents concurrently
        initial_tasks = [run_worker(tid, 0) for tid in target_ids]
        results = await asyncio.gather(*initial_tasks, return_exceptions=True)

        # Collect results (chat mode returns dict, workflow mode returns list)
        new_messages = []
        for r in results:
            if isinstance(r, list):
                new_messages.extend(r)  # workflow mode: list of messages
            elif isinstance(r, dict) and r:
                new_messages.append(r)  # chat mode: single message

        if new_messages:
            await bus.append_batch(new_messages)

        # Phase 2: Process @mention chain (sequential to maintain order)
        # Track which workers' mentions have already been processed to avoid duplicates
        processed_workers: set[int] = set()

        mention_queue = []
        for worker in self._active_workers.get(conv_id, []):
            processed_workers.add(id(worker))
            for mid in worker.mentioned_agents:
                if mid in self.agents:
                    mention_queue.append((mid, 1))

        # Process @mentions with depth tracking
        depth = 1
        while mention_queue and depth < 5 and not self._cancel_flags.get(conv_id, False):
            next_queue = []
            # Process current depth level concurrently
            tasks = []
            for agent_id, d in mention_queue:
                if response_count.get(agent_id, 0) < MAX_RESPONSES_PER_AGENT:
                    tasks.append(run_worker(agent_id, d))

            if not tasks:
                break

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect results (chat mode returns dict, workflow mode returns list)
            new_messages = []
            for r in results:
                if isinstance(r, list):
                    new_messages.extend(r)
                elif isinstance(r, dict) and r:
                    new_messages.append(r)

            if new_messages:
                await bus.append_batch(new_messages)

            # Gather @mentions ONLY from NEW workers (not already processed)
            for worker in self._active_workers.get(conv_id, []):
                if id(worker) in processed_workers:
                    continue
                processed_workers.add(id(worker))
                for mid in worker.mentioned_agents:
                    if mid in self.agents and response_count.get(mid, 0) < MAX_RESPONSES_PER_AGENT:
                        next_queue.append((mid, depth + 1))
                        event_buffer.push("mention_trigger", {
                            "from_agent": worker.agent_id,
                            "to_agent": mid,
                            "to_name": self.agents[mid]["name"],
                        })

            mention_queue = next_queue
            depth += 1

        # Done
        event_buffer.push("done", {})
        event_buffer.close()

        # Clean up
        self._active_workers.pop(conv_id, None)
        self._cancel_flags.pop(conv_id, None)

    def cut_mention_chain(self, conv_id: str):
        """Cut @mention chain only — let current workers finish, but don't spawn new ones."""
        self._cancel_flags[conv_id] = True

    def cancel_chain(self, conv_id: str):
        """Cancel all active workers and @mention chain for a conversation."""
        self._cancel_flags[conv_id] = True
        for worker in self._active_workers.get(conv_id, []):
            worker.cancel()
