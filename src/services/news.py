import asyncio
from collections import defaultdict
from typing import Callable, Any
from logging import getLogger
from functools import wraps

import aiohttp
import feedparser
from sqlalchemy import select

from src.infra.db_engine import get_connection
from src.infra.models.public import sources
from src.infra.repositories.news import NewsRepository
from src.infra.repositories.outbox import OutboxRepository
from src.infra.schemas.news import News
from src.infra.schemas.sources import Source

logger = getLogger(__name__)

def retry(attempts: int):
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for i in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception:
                    if i == attempts - 1:
                        raise
                    else:
                        await asyncio.sleep(2**i)
                        logger.info(
                            "Retrying %s (attempt %d/%d)",
                            func.__name__,
                            i + 2,
                            attempts
                        )
        return wrapper
    return decorator



class NewsService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._source_cache = defaultdict(set)
        self._news_repository = NewsRepository()
        self._outbox_repository = OutboxRepository()
        self._connection_factory = get_connection

        self._initialized = True


    async def add_news(self, source_id: int, article: News) -> bool:
        """Commit a new article; return False for an existing external ID."""
        async with self._connection_factory() as connection:
            article_id = await self._news_repository.add(
                connection, source_id, article
            )
            if article_id is None:
                return False
            await self._outbox_repository.enqueue_batch(
                connection, source_id, [article_id], [article]
            )
        return True

    async def sync_source(
        self, session: aiohttp.ClientSession, source: Source
    ) -> tuple[list[News], bool]:
        """Return (new articles, changed active IDs) after a successful commit."""
        if not source.enabled:
            return [], False
        async with self._connection_factory() as connection:
            # Serialize polls of this source, including across worker processes.
            enabled = await connection.scalar(
                select(sources.c.enabled)
                .where(sources.c.id == source.id)
                .with_for_update()
            )
            if not enabled:
                return list(), False

            feed = await self.fetch_feed(session, source.rss_url)
            if feed.get("bozo") or not feed.get("version"):
                raise ValueError(f"Invalid feed for source {source.id}")

            articles = {}
            for entry in feed.entries:
                article = News.from_entry(
                    entry, language=feed.feed.get("language")
                )
                articles.setdefault(article.external_id, article)

            old_ids = await self._news_repository.get_active_ids(
                connection, source.id
            )
            new_ids = set(articles)
            inserted_by_external_id = await self._news_repository.add_many(
                connection, source.id, list(articles.values())
            )
            inserted = [
                article
                for article in articles.values()
                if article.external_id in inserted_by_external_id
            ]
            inserted_ids = [
                inserted_by_external_id[article.external_id]
                for article in inserted
            ]
            if inserted:
                await self._outbox_repository.enqueue_batch(
                    connection, source.id, inserted_ids, inserted
                )
            await self._news_repository.set_active_ids(
                connection, source.id, new_ids
            )
        return inserted, new_ids != old_ids

    @retry(attempts=3)
    async def fetch_feed(
            self,
            session: aiohttp.ClientSession,
            url: str,
    ) -> feedparser.FeedParserDict:
        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.read()

        return await asyncio.to_thread(feedparser.parse, content)

    def update_cache(self, feed: feedparser.FeedParserDict) -> tuple[list[News], bool]:
        title = feed["feed"]["title"]
        new_entries: list[News] = []
        new_cache = set()

        entries = feed["entries"]

        for entry in entries:
            idx = entry.get("id") or entry.get("link")
            if not idx or idx in new_cache:
                continue
            if idx not in self._source_cache[title]:
                new_entries.append(News.from_entry(entry))
            new_cache.add(idx)
        need_update = new_cache != self._source_cache[title]
        self._source_cache[title] = new_cache
        return new_entries, need_update







