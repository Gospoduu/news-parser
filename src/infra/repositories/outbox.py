from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import insert, select, update, func, literal
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.types import Interval

from src.infra.models.public import outbox
from src.infra.schemas.news import News


class OutboxRepository:
    async def enqueue_batch(
        self,
        connection: AsyncConnection,
        source_id: int,
        article_ids: list[int],
        articles: list[News],
    ) -> None:
        if not article_ids or len(article_ids) != len(articles):
            raise ValueError("Outbox batch must contain matching article IDs and articles")
        event_id = uuid4()
        payload = {
            'event_id': str(event_id),
            'event_type': 'news.batch_created',
            'schema_version': 1,
            'occurred_at': datetime.now(timezone.utc).isoformat(),
            'source_id': source_id,
            'article_ids': article_ids,
            'news': [article.model_dump(mode='json') for article in articles],
        }
        await connection.execute(insert(outbox).values(
            event_id=event_id,
            # Existing column retained for backward-compatible schema migration.
            # Kafka partitioning uses source_id from the payload for a batch.
            article_id=article_ids[0],
            payload=payload,
        ))

    async def next_pending(self, connection: AsyncConnection):
        result = await connection.execute(
            select(outbox)
            .where(outbox.c.published_at.is_(None), outbox.c.available_at <= func.now())
            .order_by(outbox.c.available_at, outbox.c.created_at, outbox.c.event_id)
            .limit(1).with_for_update(skip_locked=True)
        )
        return result.mappings().first()

    async def mark_published(self, connection: AsyncConnection, event_id) -> None:
        await connection.execute(update(outbox).where(outbox.c.event_id == event_id).values(
            published_at=func.clock_timestamp(), attempts=outbox.c.attempts + 1, last_error=None,
        ))

    async def mark_failed(self, connection: AsyncConnection, event_id,
                          attempts: int, error: Exception) -> None:
        delay = min(2 ** min(attempts, 9), 300)
        await connection.execute(update(outbox).where(outbox.c.event_id == event_id).values(
            attempts=outbox.c.attempts + 1,
            available_at=func.clock_timestamp() + literal(timedelta(seconds=delay), type_=Interval()),
            last_error=f'{type(error).__name__}: {error}'[:2000],
        ))
