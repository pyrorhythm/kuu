from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import AsyncGenerator

from asyncpg import Connection, create_pool

from kuu import NotConnected
from kuu._types import _ensure_connected_cm
from ._config import PostgresConfig, PostgresDSN, PostgresParams

try:
	import asyncpg
	from asyncpg.connect_utils import _ClientConfiguration
except ImportError as e:
	raise ImportError("cannot use postgres t as `postgres` extra is not installed") from e


class PostgresTransport:
	_conn_config: _ClientConfiguration
	_pool: asyncpg.Pool

	def __init__(
			self,
			config: PostgresConfig | None = None,
			*,
			pool: asyncpg.Pool | None = None,
			conn_config: _ClientConfiguration | None = None,
	):
		self._conn_config = conn_config or _ClientConfiguration(
				statement_cache_size=100,
				max_cached_statement_lifetime=300,
				max_cacheable_statement_size=1024 * 15,
				command_timeout=None,
		)

		if pool:
			self._pool = pool
			return

		match config:
			case PostgresDSN(dsn=dsn, conn_config=conn_config):
				self._pool = create_pool(dsn=dsn)
				self._conn_config = conn_config or self._conn_config
			case PostgresParams():
				self._pool = create_pool(**asdict(config))
				self._conn_config = config.conn_config

	@asynccontextmanager
	@_ensure_connected_cm
	async def acq(self) -> AsyncGenerator[Connection]:
		if not hasattr(self, "_pool") or self._pool is None:
			raise NotConnected
		async with self._pool.acquire() as conn:
			yield conn

	async def connect(self) -> None:
		await self._pool

	async def close(self) -> None:
		await self._pool.close()
