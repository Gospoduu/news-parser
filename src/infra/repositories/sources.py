from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import sources
from src.infra.repositories.base import BaseRepository
from src.infra.schemas.sources import Source


class SourcesRepository(BaseRepository):
    model = sources
    schema_class = Source

    async def get_all(self, connection: AsyncConnection) -> list[Source]:
        """Return all sources, including disabled ones, ordered by ID."""
        result = await connection.execute(
            select(self.model).order_by(self.model.c.id)
        )
        return [self.from_row(row) for row in result.fetchall()]

    async def disable(
        self, connection: AsyncConnection, source_id: int
    ) -> Source | None:
        """Disable a source without deleting it; return None if absent."""
        result = await connection.execute(
            update(self.model)
            .where(self.model.c.id == source_id)
            .values(enabled=False)
            .returning(self.model)
        )
        row = result.fetchone()
        return self.from_row(row) if row is not None else None
    async def enable(
        self, connection: AsyncConnection, source_id: int
    ) -> Source | None:
        """Disable a source without deleting it; return None if absent."""
        result = await connection.execute(
            update(self.model)
            .where(self.model.c.id == source_id)
            .values(enabled=True)
            .returning(self.model)
        )
        row = result.fetchone()
        return self.from_row(row) if row is not None else None
