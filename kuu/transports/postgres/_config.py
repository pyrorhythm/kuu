from asyncio import AbstractEventLoop
from dataclasses import dataclass

try:
	import asyncpg
	import asyncpg.protocol
	from asyncpg.connect_utils import _ClientConfiguration
except ImportError:
	raise


class PostgresConfig: ...


@dataclass(frozen=True)
class PostgresDSN(PostgresConfig):
	dsn: str
	conn_config: _ClientConfiguration


@dataclass(frozen=True, init=True)
class PostgresParams(PostgresConfig):
	host: str | None = None
	port: int | None = None
	user: str | None = None
	password: str | None = None
	passfile: str | None = None
	service: str | None = None
	servicefile: str | None = None
	database: str | None = None
	loop: AbstractEventLoop | None = None
	timeout: float | None = 60
	statement_cache_size: int = 100
	max_cached_statement_lifetime: float = 300
	max_cacheable_statement_size: int = 1024 * 15
	command_timeout: float | None = None
	ssl = None
	direct_tls = None
	connection_class = asyncpg.Connection
	record_class = asyncpg.protocol.Record
	server_settings = None
	target_session_attrs = None
	krbsrvname = None
	gsslib = None

	@property
	def conn_config(self) -> _ClientConfiguration:
		return _ClientConfiguration(
			statement_cache_size=self.statement_cache_size,
			max_cached_statement_lifetime=self.max_cached_statement_lifetime,
			max_cacheable_statement_size=self.max_cacheable_statement_size,
			command_timeout=self.command_timeout,
		)
