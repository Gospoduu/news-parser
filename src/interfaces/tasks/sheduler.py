import asyncio
from collections.abc import Callable, Coroutine
import logging
from typing import Any

import schedule

from src.infra.schemas.sources import Source
from src.interfaces.tasks.news import ParseNewsTask, TaskManager

logger = logging.getLogger(__name__)


class BackgroundWorker:
    def __init__(
        self, task_manager: TaskManager, *,
        loop_interval_seconds: float = 1,
        refresh_interval_seconds: float = 5,
    ):
        if loop_interval_seconds <= 0 or refresh_interval_seconds <= 0:
            raise ValueError("Worker intervals must be positive")
        self.task_manager = task_manager
        self.scheduler = schedule.Scheduler()
        self.loop_interval_seconds = loop_interval_seconds
        self.refresh_interval_seconds = refresh_interval_seconds
        self.tasks: set[asyncio.Task] = set()
        self._running: dict[int, asyncio.Task] = {}
        self._jobs: dict[int, tuple[tuple[str, int], schedule.Job]] = {}
        self._loop_task: asyncio.Task | None = None
        self.event_loop = None
        self._schedule_lock = asyncio.Lock()

    async def start(self) -> asyncio.Task:
        """Load task objects, register schedule jobs and launch run_pending loop."""
        if self._loop_task is not None:
            return self._loop_task
        self.event_loop = asyncio.get_running_loop()
        await self.refresh_tasks()
        self._loop_task = self.event_loop.create_task(self.loop(), name="background-worker")
        return self._loop_task

    async def refresh_tasks(self) -> None:
        async with self._schedule_lock:
            await self._refresh_tasks()

    async def add_source(self, source: Source) -> None:
        """Register a committed source immediately; repeated calls are harmless."""
        async with self._schedule_lock:
            if source.enabled:
                self._register_task(self.task_manager.create_task(source))

    def _register_task(self, task: ParseNewsTask) -> None:
        source = task.source
        config = (source.rss_url, source.poll_interval_seconds)
        previous = self._jobs.get(source.id)
        if previous is not None:
            if previous[0] == config:
                return
            self.scheduler.cancel_job(previous[1])
        job = self.scheduler.every(source.poll_interval_seconds).seconds.do(
            self.run_task(task.run_task, source.id)
        )
        self._jobs[source.id] = (config, job)

    async def remove_source(self, source_id: int) -> None:
        """Stop future polls without interrupting an in-flight transaction/publication."""
        async with self._schedule_lock:
            previous = self._jobs.pop(source_id, None)
            if previous is not None:
                self.scheduler.cancel_job(previous[1])

    async def _refresh_tasks(self) -> None:
        objects = {task.source.id: task for task in await self.task_manager.get_tasks()}
        for source_id, (config, job) in list(self._jobs.items()):
            obj = objects.get(source_id)
            if obj is None or config != (obj.source.rss_url, obj.source.poll_interval_seconds):
                self.scheduler.cancel_job(job)
                del self._jobs[source_id]
                # Allow in-flight DB/publication work to finish. No further runs
                # of the old configuration will be scheduled.
        for source_id, obj in objects.items():
            if source_id in self._jobs:
                continue
            self._register_task(obj)

    def run_task(
        self, func: Callable[[], Coroutine[Any, Any, Any]], source_id: int
    ) -> Callable[[], None]:
        def launch() -> None:
            previous = self._running.get(source_id)
            if previous is not None and not previous.done():
                return
            task = asyncio.get_running_loop().create_task(func(), name=f"rss-source-{source_id}")
            self.tasks.add(task)
            self._running[source_id] = task
            task.add_done_callback(lambda completed: self._task_done(source_id, completed))
        return launch

    def _task_done(self, source_id: int, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        if self._running.get(source_id) is task:
            self._running.pop(source_id, None)
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Source %s task failed", source_id,
                         exc_info=(type(error), error, error.__traceback__))

    async def loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_refresh = loop.time() + self.refresh_interval_seconds
        while True:
            if loop.time() >= next_refresh:
                try:
                    await self.refresh_tasks()
                except Exception:
                    logger.exception("Failed to refresh scheduled sources")
                next_refresh = loop.time() + self.refresh_interval_seconds
            self.scheduler.run_pending()
            await asyncio.sleep(self.loop_interval_seconds)

    async def stop(self) -> None:
        if self._loop_task is not None:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None
        self.scheduler.clear()
        self._jobs.clear()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self._running.clear()
