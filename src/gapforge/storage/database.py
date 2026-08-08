"""Async PostgreSQL engine and session construction."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    """Own the SQLAlchemy engine and short-lived async sessions."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        echo: bool = False,
        pool_pre_ping: bool = True,
    ) -> Database:
        if not url.startswith(("postgresql+asyncpg://", "postgresql://")):
            raise ValueError("GapForge requires a PostgreSQL database URL")
        async_url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(async_url, echo=echo, pool_pre_ping=pool_pre_ping)
        return cls(engine)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def dispose(self) -> None:
        await self.engine.dispose()
