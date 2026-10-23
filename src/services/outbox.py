import asyncio
import logging
from collections.abc import Awaitable, Callable

from src.infra.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self, connection_factory, publish: Callable[..., Awaitable[None]], *,
                 poll_interval: float = 1, publish_timeout: float = 30):
        if poll_interval <= 0 or publish_timeout <= 0:
            raise ValueError('Outbox intervals must be positive')
        self._connection_factory = connection_factory
        self._publish = publish
        self._repository = OutboxRepository()
        self._poll_interval = poll_interval
        self._publish_timeout = publish_timeout

    async def publish_one(self) -> bool:
        async with self._connection_factory() as connection:
            event = await self._repository.next_pending(connection)
            if event is None:
                return False
            # Keep the row locked until acknowledgement and commit. Cancellation
            # rolls back; a crash after acknowledgement can cause a duplicate.
            try:
                await asyncio.wait_for(self._publish(
                    event['payload'], key=str(event['payload']['source_id']).encode(),
                ), timeout=self._publish_timeout)
            except Exception as error:
                await self._repository.mark_failed(
                    connection, event['event_id'], event['attempts'], error,
                )
                logger.warning('Outbox publication failed: event_id=%s', event['event_id'],
                               exc_info=True)
            else:
                await self._repository.mark_published(connection, event['event_id'])
        return True

    async def run(self) -> None:
        while True:
            try:
                handled = await self.publish_one()
            except Exception:
                logger.exception('Outbox transaction failed; retrying')
                handled = False
            await asyncio.sleep(0 if handled else self._poll_interval)
