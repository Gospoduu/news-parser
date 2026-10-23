"""RSS task objects and their factory; scheduling belongs to BackgroundWorker."""

from typing import TYPE_CHECKING

import aiohttp

from src.infra.schemas.sources import Source

if TYPE_CHECKING:
    from src.services.news import NewsService
    from src.services.sources import SourceService


class ParseNewsTask:
    def __init__(self, source: Source, news_service: "NewsService", session: aiohttp.ClientSession):
        self.source = source
        self.news_service = news_service
        self.session = session

    async def run_task(self) -> None:
        # NewsRepository enqueues events in the same transaction as the articles.
        await self.news_service.sync_source(self.session, self.source)


class TaskManager:
    def __init__(
        self,
        source_service: "SourceService",
        news_service: "NewsService",
        session: aiohttp.ClientSession,
    ):
        self.source_service = source_service
        self.news_service = news_service
        self.session = session

    def create_task(self, source: Source) -> ParseNewsTask:
        return ParseNewsTask(source, self.news_service, self.session)

    async def get_tasks(self) -> list[ParseNewsTask]:
        sources = await self.source_service.get_sources(refresh=True)
        return [
            self.create_task(source)
            for source in sources if source.enabled
        ]
