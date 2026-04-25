from __future__ import annotations

from redis.asyncio import Redis

from qq.exceptions import NotConnected

from .base import Result, ResultBackend


class RedisResults(ResultBackend):
    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "qq:r:"):
        self.url = url
        self.prefix = prefix
        self._r: Redis | None = None

    @property
    def r(self) -> Redis:
        if self._r is None:
            raise NotConnected("result backend not connected")
        return self._r

    def _k(self, key: str) -> str:
        return f"{self.prefix}{key}"

    async def connect(self) -> None:
        self._r = Redis.from_url(self.url)

    async def close(self) -> None:
        if self._r is not None:
            await self._r.aclose()
            self._r = None

    async def get(self, key: str) -> Result | None:
        data = await self.r.get(self._k(key))
        return Result.unmarshal(data) if data else None

    async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
        await self.r.set(self._k(key), result.marshal(), ex=int(ttl) if ttl else None)

    async def setnx(self, key: str, result: Result, ttl: float | None = None) -> bool:
        return bool(
            await self.r.set(self._k(key), result.marshal(), ex=int(ttl) if ttl else None, nx=True)
        )
