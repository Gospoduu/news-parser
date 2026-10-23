import asyncio
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI

from src.infra.db_engine import engine, get_connection, init_db
from src.interfaces.routers.sources import router as sources_router
from src.interfaces.tasks.news import TaskManager
from src.interfaces.tasks.sheduler import BackgroundWorker
from src.interfaces.streams.broker import broker
from src.interfaces.streams.news import publish_news
from src.services.news import NewsService
from src.services.sources import SourceService
from src.services.outbox import OutboxPublisher


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        ) as session:
            manager = TaskManager(SourceService(), NewsService(), session)
            worker = BackgroundWorker(manager)
            app.state.background_worker = worker
            publisher = OutboxPublisher(get_connection, publish_news)
            # Kafka may be offline at startup: parsing continues and events
            # accumulate durably until the publisher can connect.
            publisher_task = asyncio.create_task(publisher.run(), name='outbox-publisher')
            try:
                await worker.start()
                yield
            finally:
                try:
                    await worker.stop()
                finally:
                    publisher_task.cancel()
                    await asyncio.gather(publisher_task, return_exceptions=True)
    finally:
        try:
            await broker.stop()
        finally:
            await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(sources_router)
