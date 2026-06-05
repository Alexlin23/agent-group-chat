"""Orchestrator — concurrent agent scheduling for chat mode.

Spawns AgentWorkers for target agents, handles @mention chains.
Task Flow execution is handled separately by task_flow.py + workflow_engine.py.
"""

import asyncio
import uuid
from typing import Optional

from agent_worker import AgentWorker
from event_buffer import EventBuffer
from message_bus import MessageBus
from text_utils import MAX_RESPONSES_PER_AGENT, MAX_MENTION_DEPTH


class Orchestrator:
    """Manages concurrent agent processing for a conversation."""

    def __init__(
        self,
        agents: dict,
        hermes_url: str,
        hermes_key: str,
        max_concurrent: int = 3,
    ):
        self.agents = agents
        self.hermes_url = hermes_url
        self.hermes_key = hermes_key
        self.max_concurrent = max_concurrent
        self._active_workers: dict[str, list[AgentWorker]] = {}
        self._active_run: dict[str, Optional[str]] = {}

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

        # Cut previous @mention chain and register this run as active
        self.cut_mention_chain(conv_id)
        self._active_run[conv_id] = run_id
        self._active_workers[conv_id] = []

        async def run_worker(agent_id: str, depth: int) -> Optional[dict]:
            if self._active_run.get(conv_id) != run_id:
                return None
            if response_count.get(agent_id, 0) >= MAX_RESPONSES_PER_AGENT:
                return None
            if agent_id not in self.agents:
                return None

            response_count[agent_id] = response_count.get(agent_id, 0) + 1
            task_id = f"{run_id}_{agent_id}_{depth}"

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

        new_messages = []
        for r in results:
            if isinstance(r, dict) and r:
                new_messages.append(r)

        if new_messages:
            await bus.append_batch(new_messages)

        # Phase 2: Process @mention chain
        processed_workers: set[int] = set()
        mention_queue = []
        for worker in self._active_workers.get(conv_id, []):
            processed_workers.add(id(worker))
            for mid in worker.mentioned_agents:
                if mid in self.agents:
                    mention_queue.append((mid, 1))

        depth = 1
        while mention_queue and depth < MAX_MENTION_DEPTH and self._active_run.get(conv_id) == run_id:
            next_queue = []
            tasks = []
            for agent_id, d in mention_queue:
                if response_count.get(agent_id, 0) < MAX_RESPONSES_PER_AGENT:
                    tasks.append(run_worker(agent_id, d))

            if not tasks:
                break

            results = await asyncio.gather(*tasks, return_exceptions=True)

            new_messages = []
            for r in results:
                if isinstance(r, dict) and r:
                    new_messages.append(r)

            if new_messages:
                await bus.append_batch(new_messages)

            # Gather @mentions from new workers only
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

        # Only clean up if we're still the active run
        if self._active_run.get(conv_id) == run_id:
            self._active_run.pop(conv_id, None)
            self._active_workers.pop(conv_id, None)

    def cut_mention_chain(self, conv_id: str):
        """Cut @mention chain — old workers see stale run_id and stop spawning new ones."""
        self._active_run.pop(conv_id, None)

    def cancel_chain(self, conv_id: str):
        """Cancel all active workers and @mention chain."""
        self._active_run.pop(conv_id, None)
        for worker in self._active_workers.get(conv_id, []):
            worker.cancel()
