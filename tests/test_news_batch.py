import os
from contextlib import asynccontextmanager
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://news:news@localhost/news")

from feedparser import FeedParserDict

from src.infra.schemas.news import News
from src.infra.schemas.sources import Source
from src.services.news import NewsService


class NewsBatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.connection = AsyncMock()

        @asynccontextmanager
        async def transaction():
            yield self.connection

        # Bypass the application's singleton because each test needs isolated mocks.
        self.service = object.__new__(NewsService)
        self.service._connection_factory = transaction
        self.service._news_repository = AsyncMock()
        self.service._outbox_repository = AsyncMock()
        self.service.fetch_feed = AsyncMock()
        self.connection.scalar.return_value = True
        self.service._news_repository.get_active_ids.return_value = set()
        self.source = Source(
            id=1,
            name="Test source",
            rss_url="https://example.com/rss",
            poll_interval_seconds=300,
        )

    async def test_one_poll_creates_one_batch_event_for_all_new_articles(self):
        first = News(title="First", link="https://example.com/1", external_id="1")
        second = News(title="Second", link="https://example.com/2", external_id="2")
        feed = FeedParserDict(
            bozo=False,
            version="rss20",
            feed=FeedParserDict(language="ru"),
            entries=[FeedParserDict(), FeedParserDict()],
        )
        self.service.fetch_feed.return_value = feed
        self.service._news_repository.add_many.return_value = {"1": 101, "2": 102}

        with patch.object(News, "from_entry", side_effect=[first, second]):
            inserted, changed = await self.service.sync_source(object(), self.source)

        self.assertEqual(inserted, [first, second])
        self.assertTrue(changed)
        self.service._outbox_repository.enqueue_batch.assert_awaited_once_with(
            self.connection, self.source.id, [101, 102], [first, second]
        )

    async def test_poll_without_new_articles_creates_no_event(self):
        article = News(title="Known", link="https://example.com/1", external_id="1")
        feed = FeedParserDict(
            bozo=False,
            version="rss20",
            feed=FeedParserDict(),
            entries=[FeedParserDict()],
        )
        self.service.fetch_feed.return_value = feed
        self.service._news_repository.add_many.return_value = {}

        with patch.object(News, "from_entry", return_value=article):
            inserted, changed = await self.service.sync_source(object(), self.source)

        self.assertEqual(inserted, [])
        self.assertTrue(changed)
        self.service._outbox_repository.enqueue_batch.assert_not_awaited()
