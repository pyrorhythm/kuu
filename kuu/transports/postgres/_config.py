from dataclasses import dataclass

try:
	import asyncpg
	from asyncpg.connect_utils import _ClientConfiguration
	import asyncpg.protocol
except ImportError:
	raise


class PostgresConfig: ...


@dataclass(frozen=True)
class PostgresDSN(PostgresConfig):
	dsn: str
	conn_config: _ClientConfiguration


@dataclass(frozen=True)
class PostgresParams(PostgresConfig):
	"""mirrors :func:`~asyncpg.connection.connect` arguments"""

	host = None
	port = None
	user = None
	password = None
	passfile = None
	service = None
	servicefile = None
	database = None
	loop = None
	timeout = 60
	statement_cache_size = 100
	max_cached_statement_lifetime = 300
	max_cacheable_statement_size = 1024 * 15
	command_timeout = None
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
