import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from src.infra.schemas.news import News
from src.interfaces.tasks.news import ParseNewsTask, TaskManager
from src.interfaces.tasks.sheduler import BackgroundWorker


def source(id=1, enabled=True, interval=60):
    return SimpleNamespace(id=id, enabled=enabled, rss_url=f'https://example.com/{id}',
                           poll_interval_seconds=interval)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sources = AsyncMock()
        self.sources.get_sources.return_value = [source()]
        self.news = AsyncMock()
        self.news.sync_source.return_value = ([], False)
        self.manager = TaskManager(self.sources, self.news, object())
        self.worker = BackgroundWorker(self.manager, loop_interval_seconds=0.001)

    async def asyncTearDown(self):
        await self.worker.stop()

    async def test_manager_filters_and_does_not_accumulate(self):
        self.sources.get_sources.return_value = [source(), source(2, enabled=False)]
        self.assertEqual(len(await self.manager.get_tasks()), 1)
        self.assertEqual(len(await self.manager.get_tasks()), 1)
        self.sources.get_sources.assert_awaited_with(refresh=True)

    async def test_new_source_registered_immediately_once(self):
        await self.worker.start()
        new_source = source(2, interval=120)
        await self.worker.add_source(new_source)
        job = self.worker._jobs[2][1]
        await self.worker.add_source(new_source)
        self.assertIs(self.worker._jobs[2][1], job)
        self.assertEqual(len(self.worker.scheduler.jobs), 2)
        self.assertEqual(job.interval, 120)
        job.run()
        await asyncio.sleep(0.01)
        self.assertEqual(self.news.sync_source.await_args.args[1].id, 2)

    async def test_disabled_new_source_is_not_scheduled(self):
        await self.worker.add_source(source(2, enabled=False))
        self.assertFalse(self.worker.scheduler.jobs)

    async def test_disable_and_reenable_are_idempotent(self):
        await self.worker.start()
        await self.worker.remove_source(1)
        await self.worker.remove_source(1)
        self.assertFalse(self.worker.scheduler.jobs)
        await self.worker.add_source(source())
        await self.worker.add_source(source())
        self.assertEqual(len(self.worker.scheduler.jobs), 1)

    async def test_disable_preserves_inflight_task(self):
        release = asyncio.Event()

        async def waiting(*args):
            await release.wait()
            return [], False

        self.news.sync_source.side_effect = waiting
        await self.worker.start()
        self.worker.scheduler.jobs[0].run()
        await asyncio.sleep(0)
        running = next(iter(self.worker.tasks))
        await self.worker.remove_source(1)
        self.assertFalse(self.worker.scheduler.jobs)
        self.assertFalse(running.done())
        release.set()
        await running
        self.assertFalse(running.cancelled())

    async def test_disable_waits_for_inflight_refresh(self):
        reading = asyncio.Event()
        release = asyncio.Event()

        async def stale_snapshot(**kwargs):
            reading.set()
            await release.wait()
            return [source()]

        self.sources.get_sources.side_effect = stale_snapshot
        refresh = asyncio.create_task(self.worker.refresh_tasks())
        await reading.wait()
        removing = asyncio.create_task(self.worker.remove_source(1))
        release.set()
        await asyncio.gather(refresh, removing)
        self.assertFalse(self.worker.scheduler.jobs)

    async def test_add_waits_for_inflight_refresh(self):
        reading = asyncio.Event()
        release = asyncio.Event()

        async def stale_snapshot(**kwargs):
            reading.set()
            await release.wait()
            return [source()]

        self.sources.get_sources.side_effect = stale_snapshot
        refresh = asyncio.create_task(self.worker.refresh_tasks())
        await reading.wait()
        adding = asyncio.create_task(self.worker.add_source(source(2)))
        await asyncio.sleep(0)
        self.assertFalse(adding.done())
        release.set()
        await asyncio.gather(refresh, adding)
        self.assertEqual(set(self.worker._jobs), {1, 2})

    async def test_schedule_launch_and_cleanup(self):
        loop_task = await self.worker.start()
        self.assertIs(await self.worker.start(), loop_task)
        self.assertEqual(len(self.worker.scheduler.jobs), 1)
        job = self.worker.scheduler.jobs[0]
        self.assertEqual(job.interval, 60)
        job.next_run = datetime.now() - timedelta(seconds=1)
        self.worker.scheduler.run_pending()
        await asyncio.sleep(0.01)
        self.news.sync_source.assert_awaited_once()
        self.assertFalse(self.worker.tasks)
        await self.worker.stop()
        self.assertTrue(loop_task.done())
        self.assertFalse(self.worker.scheduler.jobs)

    async def test_refresh_disable_create_and_interval(self):
        await self.worker.start()
        self.sources.get_sources.return_value = [source(interval=120), source(2)]
        await self.worker.refresh_tasks()
        self.assertEqual(set(self.worker._jobs), {1, 2})
        self.assertEqual(self.worker._jobs[1][1].interval, 120)
        self.sources.get_sources.return_value = [source(enabled=False), source(2)]
        await self.worker.refresh_tasks()
        self.assertEqual(set(self.worker._jobs), {2})

    async def test_overlap_skipped_and_stop_cancels_running(self):
        event = asyncio.Event()
        self.news.sync_source.side_effect = lambda *args: None
        async def wait(*args):
            await event.wait()
            return [], False
        self.news.sync_source.side_effect = wait
        await self.worker.start()
        job = self.worker.scheduler.jobs[0]
        job.run()
        job.run()
        await asyncio.sleep(0)
        self.news.sync_source.assert_awaited_once()
        running = list(self.worker.tasks)
        await self.worker.stop()
        self.assertTrue(all(task.cancelled() for task in running))

    async def test_failure_logged_and_job_can_run_again(self):
        await self.worker.start()
        self.news.sync_source.side_effect = RuntimeError('temporary')
        with self.assertLogs('src.interfaces.tasks.sheduler', level='ERROR'):
            self.worker.scheduler.jobs[0].run()
            await asyncio.sleep(0.01)
        self.assertFalse(self.worker.tasks)
        self.news.sync_source.side_effect = None
        self.worker.scheduler.jobs[0].run()
        await asyncio.sleep(0.01)
        self.assertEqual(self.news.sync_source.await_count, 2)

    async def test_parse_task_only_syncs_database(self):
        article = News(title='Title', link='https://example.com/a', external_id='a')
        self.news.sync_source.return_value = ([article], True)
        task = ParseNewsTask(source(), self.news, object())
        await task.run_task()
        self.news.sync_source.assert_awaited_once_with(task.session, task.source)
        self.news.sync_source.side_effect = RuntimeError('database unavailable')
        with self.assertRaises(RuntimeError):
            await task.run_task()


if __name__ == '__main__':
    unittest.main()
