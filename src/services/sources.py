import asyncio

from src.infra.db_engine import get_connection
from src.infra.repositories.sources import SourcesRepository
from src.infra.schemas.sources import Source, SourceCreate


class SourceService:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._sources_repository = SourcesRepository()
        self._connection_factory = get_connection
        self._sources: list[Source] | None = None
        self._lock = asyncio.Lock()

        self._initialized = True

    async def create_source(self, source: SourceCreate) -> Source:
        async with self._lock:
            async with self._connection_factory() as connection:
                created = await self._sources_repository.create(
                    connection,
                    **source.model_dump(),
                )
            self._sources = None
            return created

    async def disable_source(self, source_id: int) -> Source | None:
        async with self._lock:
            async with self._connection_factory() as connection:
                source = await self._sources_repository.disable(
                    connection,
                    source_id,
                )
            self._sources = None
            return source

    async def enable_source(self, source_id: int) -> Source | None:
        async with self._lock:
            async with self._connection_factory() as connection:
                source = await self._sources_repository.enable(connection, source_id)
            self._sources = None
            return source

    async def get_sources(self, *, refresh: bool = False) -> list[Source]:
        async with self._lock:
            if refresh or self._sources is None:
                async with self._connection_factory() as connection:
                    sources = await self._sources_repository.get_all(
                        connection,
                    )
                self._sources = sources
            return [
                source.model_copy(deep=True)
                for source in self._sources
            ]
