from contextlib import AbstractAsyncContextManager
from dataclasses import asdict

from asyncpg import create_pool

from kuu import NotConnected
from ._config import PostgresConfig, PostgresDSN, PostgresParams

try:
	import asyncpg
	from asyncpg.connect_utils import _ClientConfiguration
except ImportError as e:
	raise ImportError("cannot use postgres transport as `postgres` extra is not installed") from e


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
			self.__pool = pool
			return

		match config:
			case PostgresDSN(dsn=dsn, conn_config=conn_config):
				self._pool = create_pool(dsn=dsn)
				self._conn_config = conn_config or self._conn_config
			case PostgresParams():
				self._pool = create_pool(**asdict(config))
				self._conn_config = config.conn_config

	def acq(self) -> AbstractAsyncContextManager[asyncpg.Connection]:
		if not hasattr(self, "_pool") or self._pool is None:
			raise NotConnected
		return self._pool.acquire()

	async def close(self) -> None:
		await self._pool.close()
