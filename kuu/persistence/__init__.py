from __future__ import annotations

from kuu.config import PersistenceConfig
from kuu.persistence._backend import NoopBackend, PersistenceBackend
from kuu.persistence._rows import LogRow, RunRow
from kuu.persistence._worker import PersistenceWorker


def create_backend(cfg: PersistenceConfig) -> PersistenceBackend:
	if not cfg.enable:
		return NoopBackend()

	dsn = cfg.dsn
	if dsn.startswith("sqlite"):
		from kuu.persistence._sqlite import SqliteBackend

		return SqliteBackend(cfg)

	if dsn.startswith("postgres://") or dsn.startswith("postgresql://"):
		from kuu.persistence._postgres import PostgresBackend

		return PostgresBackend(cfg)

	raise ValueError(
		f"unsupported persistence DSN scheme: {dsn.split('://')[0] if '://' in dsn else dsn}"
	)


__all__ = [
	"PersistenceConfig",
	"PersistenceBackend",
	"PersistenceWorker",
	"NoopBackend",
	"RunRow",
	"LogRow",
	"create_backend",
]
