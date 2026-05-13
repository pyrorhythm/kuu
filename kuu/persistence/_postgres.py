from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Self

from asyncpg import Connection, Pool, exceptions
from asyncpg.pool import PoolAcquireContext
from asyncpg.protocol import Record

from kuu.config import PersistenceConfig
from kuu.marshal import marshal as _m
from kuu.persistence._backend import PersistenceBackend
from kuu.persistence._rows import (
	LogRow,
	RunRow,
	parse_pg_dsn,
	to_naive,
	validate_table_name,
)


class _PoolAcqCtxProxy(PoolAcquireContext):
	async def __aenter__(self) -> Connection:
		if self.connection is not None or self.done:
			raise exceptions.InterfaceError("a connection is already acquired")
		conn: Connection = await self.pool._acquire(self.timeout)
		for t in ("jsonb", "json"):
			await conn.set_type_codec(
				typename=t,
				schema="pg_catalog",
				encoder=_m.json_encode_str,
				decoder=_m.json_decode,
			)
		self.connection = conn
		return self.connection


class _PoolProxy(Pool):
	@classmethod
	def _create(
		cls,
		dsn=None,
		*,
		min_size=10,
		max_size=10,
		max_queries=50000,
		max_inactive_connection_lifetime=300.0,
		connect=None,
		setup=None,
		init=None,
		reset=None,
		loop=None,
		connection_class=Connection,
		record_class=Record,
		**connect_kwargs,
	) -> Self:
		inst = cls(
			dsn,
			connection_class=connection_class,
			record_class=record_class,
			min_size=min_size,
			max_size=max_size,
			max_queries=max_queries,
			loop=loop,
			connect=connect,
			setup=setup,
			init=init,
			reset=reset,
			max_inactive_connection_lifetime=max_inactive_connection_lifetime,
			**connect_kwargs,
		)
		return inst

	def acquire(self, *, timeout=None) -> PoolAcquireContext:
		return _PoolAcqCtxProxy(self, timeout)


log = logging.getLogger("kuu.persistence.postgres")


class PostgresBackend(PersistenceBackend):
	def __init__(self, cfg: PersistenceConfig) -> None:
		self._cfg = cfg
		self._dsn = parse_pg_dsn(cfg.dsn)
		validate_table_name(cfg.runs_table)
		validate_table_name(cfg.logs_table)
		if cfg.schema is not None:
			validate_table_name(cfg.schema)
		self._pool: Pool | None = None
		self._qualified_runs: str = ""
		self._qualified_logs: str = ""

	def _qualify(self, table: str) -> str:
		schema = self._cfg.schema
		if schema:
			return f'"{schema}"."{table}"'
		return f'"{table}"'

	async def connect(self) -> None:
		pool = await _PoolProxy._create(
			self._dsn,
			min_size=1,
			max_size=3,
		)
		if pool is None:
			raise RuntimeError("asyncpg.create_pool returned None")
		self._pool = pool
		self._qualified_runs = self._qualify(self._cfg.runs_table)
		self._qualified_logs = self._qualify(self._cfg.logs_table)

	async def close(self) -> None:
		if self._pool is not None:
			await self._pool.close()
			self._pool = None

	async def init_schema(self) -> None:
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			if self._cfg.schema:
				await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._cfg.schema}"')
			rt = self._qualified_runs
			lt = self._qualified_logs
			await conn.execute(f"""CREATE TABLE IF NOT EXISTS {rt} (
			    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			    message_id TEXT NOT NULL,
			    attempt INTEGER NOT NULL DEFAULT 0,
			    task TEXT NOT NULL,
			    queue TEXT NOT NULL DEFAULT 'default',
			    instance_id TEXT NOT NULL,
			    worker_pid INTEGER NOT NULL DEFAULT 0,

			    status TEXT NOT NULL DEFAULT 'succeeded'
			        CHECK (status IN (
			            'enqueued', 'started',
			            'succeeded', 'failed',
			            'retried', 'dead'
			        )),

			    args JSONB,
			    kwargs JSONB,
			    started_at TIMESTAMP,
			    finished_at TIMESTAMP,
			    time_elapsed INTERVAL,
			    exc_type TEXT,
			    exc_message TEXT,
			    traceback TEXT,

					UNIQUE (message_id, attempt)
			)""")
			await conn.execute(f"""CREATE TABLE IF NOT EXISTS {lt} (
			    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			    message_id TEXT NOT NULL,
			    attempt INTEGER NOT NULL DEFAULT 0,
			    ts TIMESTAMP NOT NULL DEFAULT NOW(),
			    level INTEGER NOT NULL DEFAULT 0,
			    logger TEXT NOT NULL DEFAULT '',
			    message TEXT NOT NULL DEFAULT '',

			    FOREIGN KEY (message_id, attempt)
			        REFERENCES {rt} (message_id, attempt)
			        ON DELETE CASCADE
			)""")

			idx_specs = [
				(f"{self._cfg.runs_table}_finished_at_idx", f"{rt}(finished_at DESC)"),
				(
					f"{self._cfg.runs_table}_task_status_idx",
					f"{rt}(task, status, finished_at DESC)",
				),
				(
					f"{self._cfg.logs_table}_message_id_attempt_ts_idx",
					f"{lt}(message_id, attempt, ts)",
				),
			]
			for name, spec in idx_specs:
				await conn.execute(f'CREATE INDEX IF NOT EXISTS "{name}" ON {spec}')

	_RUNS_COLS = (
		"id, message_id, attempt, task, queue, instance_id, "
		"worker_pid, args, kwargs, started_at, finished_at, "
		"time_elapsed, status, exc_type, exc_message, traceback"
	)

	async def write_runs(self, runs: list[RunRow]) -> None:
		if not runs:
			return
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			await conn.executemany(
				f"""
					INSERT INTO {self._qualified_runs} (
						message_id, attempt, task, queue, instance_id, worker_pid,
						args, kwargs, started_at, finished_at,
						time_elapsed, status, exc_type, exc_message, traceback
					) VALUES (
					  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
					)
					ON CONFLICT (message_id, attempt) DO UPDATE SET
						finished_at = EXCLUDED.finished_at,
						time_elapsed = EXCLUDED.time_elapsed,
						status = EXCLUDED.status,
						exc_type = EXCLUDED.exc_type,
						exc_message = EXCLUDED.exc_message,
						traceback = EXCLUDED.traceback
				""",
				[r.astuple() for r in runs],
			)

	async def write_logs(self, logs: list[LogRow]) -> None:
		if not logs:
			return
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			await conn.executemany(
				f"""
				  INSERT INTO {self._qualified_logs}
				  (message_id, attempt, ts, level, logger, message)
					VALUES ($1, $2, $3, $4, $5, $6)
				""",
				[lr.astuple() for lr in logs],
			)

	async def query_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		before: datetime | None = None,
		after: datetime | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[RunRow]:
		assert self._pool is not None
		rt = self._qualified_runs
		where: list[str] = []
		params: list[Any] = []
		idx = 1
		if task is not None:
			where.append(f"task = ${idx}")
			params.append(task)
			idx += 1
		if status is not None:
			where.append(f"status = ${idx}")
			params.append(status)
			idx += 1
		if before is not None:
			where.append(f"finished_at <= ${idx}")
			params.append(to_naive(before))
			idx += 1
		if after is not None:
			where.append(f"finished_at >= ${idx}")
			params.append(to_naive(after))
			idx += 1
		clause = " AND ".join(where) if where else "TRUE"
		params.append(limit)
		params.append(offset)
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""
					SELECT {self._RUNS_COLS} FROM {rt}
					WHERE {clause}
					ORDER BY finished_at DESC NULLS LAST
					LIMIT ${idx} OFFSET ${idx + 1}
				""",
				*params,
			)
		return [RunRow.fromrecord(r) for r in rows]

	async def query_run_attempts(self, message_id: str) -> list[RunRow]:
		assert self._pool is not None
		rt = self._qualified_runs
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""
				  SELECT {self._RUNS_COLS}
					FROM {rt} WHERE message_id = $1
					ORDER BY attempt ASC
				""",
				message_id,
			)
		return [RunRow.fromrecord(r) for r in rows]

	async def query_logs(
		self,
		message_id: str,
		attempt: int,
		*,
		after_dt: datetime | None = None,
		limit: int = 500,
	) -> list[LogRow]:
		assert self._pool is not None
		lt = self._qualified_logs

		where = "message_id = $2 AND attempt = $3"
		params: list = [message_id, attempt]
		if after_dt:
			where += " AND ts > $4"
			params.append(to_naive(after_dt))

		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""
				  SELECT message_id, attempt, ts, level, logger, message
					FROM {lt}
					WHERE {where}
					ORDER BY ts ASC LIMIT $1
				""",
				limit,
				*params,
			)
		return [LogRow.fromrecord(r) for r in rows]

	async def prune(self, before_ts: datetime) -> int:
		assert self._pool is not None
		rt = self._qualified_runs
		lt = self._qualified_logs
		async with self._pool.acquire() as conn:
			await conn.execute(
				f"""DELETE FROM {lt}
					WHERE (message_id, attempt) IN
					(SELECT message_id, attempt FROM {rt} WHERE finished_at < $1)""",
				to_naive(before_ts),
			)
			result = await conn.execute(
				f"DELETE FROM {rt} WHERE finished_at < $1",
				to_naive(before_ts),
			)
		parts = result.split()
		return int(parts[1]) if len(parts) > 1 else 0
