from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import news
from src.infra.repositories.base import BaseRepository
from src.infra.schemas.news import News


class NewsRepository(BaseRepository):
    model = news
    schema_class = News

    async def add(
        self, connection: AsyncConnection, source_id: int, article: News
    ) -> int | None:
        """Insert once and return its database ID; return None for a duplicate."""
        return (await self.add_many(connection, source_id, [article])).get(
            article.external_id
        )

    async def add_many(
        self,
        connection: AsyncConnection,
        source_id: int,
        articles: list[News],
    ) -> dict[str, int]:
        """Insert a feed batch and map newly inserted external IDs to database IDs."""
        if not articles:
            return {}
        result = await connection.execute(
            insert(self.model)
            .values([
                {"source_id": source_id, **article.model_dump()}
                for article in articles
            ])
            .on_conflict_do_nothing(
                constraint="uq_news_source_external_id"
            )
            .returning(self.model.c.id, self.model.c.external_id)
        )
        return {
            row.external_id: row.id
            for row in result.fetchall()
        }

    async def get_active_ids(
        self, connection: AsyncConnection, source_id: int
    ) -> set[str]:
        result = await connection.execute(
            select(self.model.c.external_id).where(
                self.model.c.source_id == source_id,
                self.model.c.is_active.is_(True),
            )
        )
        return set(result.scalars())

    async def set_active_ids(
        self, connection: AsyncConnection, source_id: int, ids: set[str]
    ) -> None:
        active = self.model.c.external_id.in_(ids)
        await connection.execute(
            update(self.model)
            .where(
                self.model.c.source_id == source_id,
                self.model.c.is_active.is_distinct_from(active),
            )
            .values(is_active=active)
        )
