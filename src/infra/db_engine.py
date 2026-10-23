# src.infra.db_engine
from contextlib import asynccontextmanager
import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import  create_async_engine

from src.infra.models.public import metadata

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise ValueError("DATABASE_URL не задан в .env")

engine = create_async_engine(
    db_url,
    future=True,
    pool_pre_ping=True,
)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

@asynccontextmanager
async def get_connection() -> AsyncGenerator:
    async with engine.begin() as conn:
        yield conn