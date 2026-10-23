from logging import getLogger
import os
from typing import Any

from src.interfaces.streams.broker import broker

logger = getLogger(__name__)
NEWS_TOPIC = os.getenv('NEWS_TOPIC')
if not NEWS_TOPIC:
    raise RuntimeError('NEWS_TOPIC is not configured')


async def publish_news(event: dict[str, Any], *, key: bytes) -> None:
    # connect() reuses the producer once connected; on initial failure the
    # outbox retains the event and calls us again after backoff.
    await broker.connect()
    await broker.publish(event, topic=NEWS_TOPIC, key=key, no_confirm=False)
    logger.info('Published event_id=%s', event['event_id'])
