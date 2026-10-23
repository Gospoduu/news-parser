import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy.dialects.postgresql import dialect

from src.infra.repositories.news import NewsRepository
from src.infra.repositories.outbox import OutboxRepository
from src.infra.schemas.news import News
from src.services.outbox import OutboxPublisher


class OutboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.committed = False
        self.rolled_back = False
        self.connection = AsyncMock()

        @asynccontextmanager
        async def transaction():
            try:
                yield self.connection
            except BaseException:
                self.rolled_back = True
                raise
            else:
                self.committed = True

        self.publish = AsyncMock()
        self.sender = OutboxPublisher(transaction, self.publish)
        self.repo = AsyncMock()
        self.sender._repository = self.repo
        self.event = dict(event_id=uuid4(), article_id=12, attempts=0,
                          payload={'event_id': 'stable-id', 'source_id': 3, 'news': []})
        self.repo.next_pending.return_value = self.event

    async def test_success_marks_after_ack(self):
        async def ack(*args, **kwargs):
            self.repo.mark_published.assert_not_awaited()
            self.assertFalse(self.committed)
        self.publish.side_effect = ack
        self.assertTrue(await self.sender.publish_one())
        self.repo.mark_published.assert_awaited_once_with(self.connection, self.event['event_id'])
        self.assertTrue(self.committed)
        self.publish.assert_awaited_once_with(self.event['payload'], key=b'3')

    async def test_failure_is_persisted_and_retry_keeps_payload(self):
        self.publish.side_effect = RuntimeError('offline')
        with self.assertLogs('src.services.outbox', level='WARNING'):
            await self.sender.publish_one()
        self.repo.mark_failed.assert_awaited_once()
        self.repo.mark_published.assert_not_awaited()
        self.assertTrue(self.committed)
        first_payload = self.publish.await_args.args[0]
        self.publish.side_effect = None
        await self.sender.publish_one()
        self.assertEqual(first_payload, self.publish.await_args.args[0])
        self.repo.mark_published.assert_awaited_once()

    async def test_cancellation_rolls_back_without_marking(self):
        self.publish.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.sender.publish_one()
        self.assertTrue(self.rolled_back)
        self.repo.mark_failed.assert_not_awaited()
        self.repo.mark_published.assert_not_awaited()

    async def test_timeout_records_failure(self):
        self.sender._publish_timeout = 0.001
        async def hang(*args, **kwargs):
            await asyncio.Event().wait()
        self.publish.side_effect = hang
        with self.assertLogs('src.services.outbox', level='WARNING'):
            await self.sender.publish_one()
        self.repo.mark_failed.assert_awaited_once()
        self.repo.mark_published.assert_not_awaited()

    async def test_idle_does_not_publish(self):
        self.repo.next_pending.return_value = None
        self.assertFalse(await self.sender.publish_one())
        self.publish.assert_not_awaited()

    async def test_insert_returns_id_and_batch_enqueues_all_articles(self):
        result = MagicMock()
        result.fetchall.return_value = [
            SimpleNamespace(id=12, external_id="a"),
        ]
        self.connection.execute.return_value = result
        first = News(title='First', link='https://example.com/a', external_id='a')
        second = News(title='Second', link='https://example.com/b', external_id='b')
        repository = NewsRepository()
        self.assertEqual(await repository.add(self.connection, 3, first), 12)
        self.assertEqual(self.connection.execute.await_count, 1)
        statement = self.connection.execute.await_args.args[0]
        self.assertIn("VALUES", str(statement))
        self.connection.reset_mock()
        result.fetchall.return_value = [
            SimpleNamespace(id=12, external_id="a"),
            SimpleNamespace(id=13, external_id="b"),
        ]
        self.assertEqual(
            await repository.add_many(self.connection, 3, [first, second]),
            {"a": 12, "b": 13},
        )
        self.assertEqual(self.connection.execute.await_count, 1)
        await OutboxRepository().enqueue_batch(
            self.connection, 3, [12, 13], [first, second]
        )
        self.assertEqual(self.connection.execute.await_count, 2)
        outbox_insert = self.connection.execute.await_args.args[0]
        payload = outbox_insert.compile().params['payload']
        self.assertEqual(payload['source_id'], 3)
        self.assertEqual(payload['article_ids'], [12, 13])
        self.assertEqual(
            [article['external_id'] for article in payload['news']], ['a', 'b']
        )
        self.assertEqual(payload['event_type'], 'news.batch_created')
        self.assertIn('event_id', payload)
        self.connection.reset_mock()
        result.fetchall.return_value = []
        self.assertIsNone(await repository.add(self.connection, 3, first))
        self.assertEqual(self.connection.execute.await_count, 1)

    async def test_batch_rejects_empty_or_mismatched_data(self):
        repository = OutboxRepository()
        article = News(title='Test', link='https://example.com/a', external_id='a')
        with self.assertRaises(ValueError):
            await repository.enqueue_batch(self.connection, 3, [], [])
        with self.assertRaises(ValueError):
            await repository.enqueue_batch(self.connection, 3, [12], [article, article])
        self.connection.execute.assert_not_awaited()

    async def test_claim_sql_skips_locked_and_only_due_unsent(self):
        self.connection.execute.return_value = MagicMock()
        await OutboxRepository().next_pending(self.connection)
        sql = str(self.connection.execute.await_args.args[0].compile(dialect=dialect()))
        self.assertIn('FOR UPDATE SKIP LOCKED', sql)
        self.assertIn('published_at IS NULL', sql)
        self.assertIn('available_at <= now()', sql)

    async def test_backoff_capped(self):
        await OutboxRepository().mark_failed(self.connection, uuid4(), 1000, RuntimeError('offline'))
        params = self.connection.execute.await_args.args[0].compile().params
        self.assertIn(300, [v.total_seconds() for v in params.values() if hasattr(v, 'total_seconds')])
