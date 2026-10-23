"""Optional real-PostgreSQL tests. Set TEST_DATABASE_URL to an asyncpg URL.

Each test creates and removes its own schema; existing application tables are
never touched. Kafka is replaced with an acknowledged/failing async publisher.
"""
import os
from contextlib import asynccontextmanager
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from src.infra.models.public import metadata, news, outbox, sources
from src.infra.repositories.news import NewsRepository
from src.infra.repositories.outbox import OutboxRepository
from src.infra.schemas.news import News
from src.services.outbox import OutboxPublisher


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'TEST_DATABASE_URL is not configured')
class PostgresOutboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = 'test_outbox_' + uuid4().hex
        self.engine = create_async_engine(os.environ['TEST_DATABASE_URL'],
                                         execution_options={'schema_translate_map': {'public': self.schema}})
        async with self.engine.begin() as conn:
            await conn.execute(CreateSchema(self.schema))
            await conn.run_sync(metadata.create_all)
            self.source_id = await conn.scalar(sources.insert().values(
                name='Test', rss_url='https://example.com/rss').returning(sources.c.id))
        self.article = News(title='Test', link='https://example.com/a', external_id='a')

        @asynccontextmanager
        async def connection():
            async with self.engine.begin() as conn:
                yield conn
        self.connection = connection

    async def asyncTearDown(self):
        try:
            async with self.engine.begin() as conn:
                await conn.execute(DropSchema(self.schema, cascade=True))
        finally:
            await self.engine.dispose()

    async def test_atomicity_and_deduplication(self):
        with patch.object(OutboxRepository, 'enqueue_batch', side_effect=RuntimeError('enqueue failed')):
            with self.assertRaises(RuntimeError):
                async with self.connection() as conn:
                    article_id = await NewsRepository().add(conn, self.source_id, self.article)
                    await OutboxRepository().enqueue_batch(
                        conn, self.source_id, [article_id], [self.article]
                    )
        async with self.connection() as conn:
            self.assertEqual(await conn.scalar(select(func.count()).select_from(news)), 0)
            self.assertEqual(await conn.scalar(select(func.count()).select_from(outbox)), 0)
            article_id = await NewsRepository().add(conn, self.source_id, self.article)
            await OutboxRepository().enqueue_batch(
                conn, self.source_id, [article_id], [self.article]
            )
            self.assertIsNone(await NewsRepository().add(conn, self.source_id, self.article))
        async with self.connection() as conn:
            self.assertEqual(await conn.scalar(select(func.count()).select_from(news)), 1)
            self.assertEqual(await conn.scalar(select(func.count()).select_from(outbox)), 1)

    async def test_failure_restart_and_success(self):
        async with self.connection() as conn:
            article_id = await NewsRepository().add(conn, self.source_id, self.article)
            await OutboxRepository().enqueue_batch(
                conn, self.source_id, [article_id], [self.article]
            )
        publish = AsyncMock(side_effect=RuntimeError('Kafka unavailable'))
        sender = OutboxPublisher(self.connection, publish)
        with self.assertLogs('src.services.outbox', level='WARNING'):
            await sender.publish_one()
        async with self.connection() as conn:
            row = (await conn.execute(select(outbox))).mappings().one()
            self.assertIsNone(row['published_at'])
            self.assertEqual(row['attempts'], 1)
            event_id = row['event_id']
            await conn.execute(update(outbox).values(available_at=func.now()))
        publish.side_effect = None
        # New instance simulates restarting the publisher process.
        sender = OutboxPublisher(self.connection, publish)
        self.assertTrue(await sender.publish_one())
        self.assertFalse(await sender.publish_one())
        async with self.connection() as conn:
            row = (await conn.execute(select(outbox))).mappings().one()
            self.assertEqual(row['event_id'], event_id)
            self.assertIsNotNone(row['published_at'])
        self.assertEqual(publish.await_args_list[0], publish.await_args_list[1])

    async def test_two_publishers_skip_locked_row(self):
        async with self.connection() as conn:
            article_id = await NewsRepository().add(conn, self.source_id, self.article)
            await OutboxRepository().enqueue_batch(
                conn, self.source_id, [article_id], [self.article]
            )
        async with self.connection() as first:
            claimed = await OutboxRepository().next_pending(first)
            self.assertIsNotNone(claimed)
            async with self.connection() as second:
                self.assertIsNone(await OutboxRepository().next_pending(second))
