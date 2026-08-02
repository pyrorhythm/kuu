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
	DashboardStats,
	LogRow,
	LogicalRunRow,
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
		validate_table_name(cfg.logical_runs_table)
		validate_table_name(cfg.logs_table)
		if cfg.schema is not None:
			validate_table_name(cfg.schema)
		self._pool: Pool | None = None
		self._qualified_runs: str = ""
		self._qualified_logical_runs: str = ""
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
		self._qualified_logical_runs = self._qualify(self._cfg.logical_runs_table)
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
			lrt = self._qualified_logical_runs
			lt = self._qualified_logs
			await conn.execute(f"""CREATE TABLE IF NOT EXISTS {lrt} (
			    message_id TEXT PRIMARY KEY,
			    task TEXT NOT NULL,
			    queue TEXT NOT NULL,
			    instance_id TEXT NOT NULL,
			    status TEXT NOT NULL CHECK (status IN (
			        'enqueued', 'running', 'cancel_requested',
			        'succeeded', 'failed', 'cancelled', 'unknown'
			    )),
			    created_at TIMESTAMP NOT NULL,
			    updated_at TIMESTAMP NOT NULL,
			    replay_of TEXT,
			    attempt_count INTEGER NOT NULL DEFAULT 1,
			    dead_lettered BOOLEAN NOT NULL DEFAULT FALSE
			)""")
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
			            'retried', 'dead', 'cancelled'
			        )),

			    args JSONB,
			    kwargs JSONB,
			    headers JSONB,
			    input_complete BOOLEAN NOT NULL DEFAULT FALSE,
			    result_preview JSONB,
			    started_at TIMESTAMP,
			    finished_at TIMESTAMP,
			    time_elapsed INTERVAL,
			    exc_type TEXT,
			    exc_message TEXT,
			    traceback TEXT,
			    failure JSONB,
			    state_override TEXT CHECK (state_override IN ('unknown', 'lost')),

					UNIQUE (message_id, attempt)
			)""")
			await conn.execute(f"""CREATE TABLE IF NOT EXISTS {lt} (
			    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			    message_id TEXT NOT NULL,
			    attempt INTEGER NOT NULL DEFAULT 0,
			    ts TIMESTAMP NOT NULL DEFAULT NOW(),
			    kind TEXT NOT NULL DEFAULT 'log',
			    seq BIGINT NOT NULL DEFAULT 0,
			    level INTEGER NOT NULL DEFAULT 0,
			    logger TEXT NOT NULL DEFAULT '',
			    message TEXT NOT NULL DEFAULT '',
			    fields JSONB,
			    current DOUBLE PRECISION,
			    total DOUBLE PRECISION,
			    dropped BIGINT NOT NULL DEFAULT 0,

			    FOREIGN KEY (message_id, attempt)
			        REFERENCES {rt} (message_id, attempt)
			        ON DELETE CASCADE
			)""")

			await conn.execute(f"ALTER TABLE {rt} ADD COLUMN IF NOT EXISTS headers JSONB")
			await conn.execute(
				f"ALTER TABLE {rt} ADD COLUMN IF NOT EXISTS input_complete "
				"BOOLEAN NOT NULL DEFAULT FALSE"
			)
			await conn.execute(f"ALTER TABLE {rt} ADD COLUMN IF NOT EXISTS result_preview JSONB")
			await conn.execute(f"ALTER TABLE {rt} ADD COLUMN IF NOT EXISTS failure JSONB")
			await conn.execute(f"ALTER TABLE {rt} ADD COLUMN IF NOT EXISTS state_override TEXT")
			await self._migrate_run_status_constraint(conn, rt)
			await self._backfill_logical_runs(conn, rt, lrt)

			for column in (
				"kind TEXT NOT NULL DEFAULT 'log'",
				"seq BIGINT NOT NULL DEFAULT 0",
				"fields JSONB",
				"current DOUBLE PRECISION",
				"total DOUBLE PRECISION",
				"dropped BIGINT NOT NULL DEFAULT 0",
			):
				await conn.execute(f"ALTER TABLE {lt} ADD COLUMN IF NOT EXISTS {column}")

			idx_specs = [
				(
					f"{self._cfg.logical_runs_table}_updated_at_idx",
					f"{lrt}(updated_at DESC)",
				),
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

	async def _migrate_run_status_constraint(self, conn: Any, rt: str) -> None:
		qualified_name = (
			f"{self._cfg.schema}.{self._cfg.runs_table}"
			if self._cfg.schema
			else self._cfg.runs_table
		)
		constraints = await conn.fetch(
			"""SELECT DISTINCT constraint_row.conname
				FROM pg_constraint AS constraint_row
				JOIN pg_attribute AS attribute_row
					ON attribute_row.attrelid = constraint_row.conrelid
					AND attribute_row.attnum = ANY(constraint_row.conkey)
				WHERE constraint_row.conrelid = $1::regclass
					AND constraint_row.contype = 'c'
					AND attribute_row.attname = 'status'""",
			qualified_name,
		)
		for row in constraints:
			name = row["conname"].replace('"', '""')
			await conn.execute(f'ALTER TABLE {rt} DROP CONSTRAINT "{name}"')
		await conn.execute(f"""ALTER TABLE {rt}
			ADD CONSTRAINT kuu_run_status_check CHECK (status IN (
				'enqueued', 'started', 'succeeded', 'failed',
				'retried', 'dead', 'cancelled'
			))""")

	async def _backfill_logical_runs(self, conn: Any, rt: str, lrt: str) -> None:
		await conn.execute(f"""
			WITH ranked AS (
				SELECT attempt_row.*,
					ROW_NUMBER() OVER (
						PARTITION BY message_id ORDER BY attempt DESC, id DESC
					) AS row_number,
					MAX(attempt) OVER (PARTITION BY message_id) + 1 AS attempt_count,
					MIN(COALESCE(started_at, finished_at)) OVER (
						PARTITION BY message_id
					) AS created_at_value,
					MAX(COALESCE(finished_at, started_at)) OVER (
						PARTITION BY message_id
					) AS updated_at_value,
					BOOL_OR(status = 'dead') OVER (
						PARTITION BY message_id
					) AS dead_lettered_value
				FROM {rt} AS attempt_row
			)
			INSERT INTO {lrt} (
				message_id, task, queue, instance_id, status, created_at,
				updated_at, replay_of, attempt_count, dead_lettered
			)
			SELECT message_id, task, queue, instance_id,
				CASE
					WHEN state_override IN ('unknown', 'lost') THEN 'unknown'
					WHEN status = 'enqueued' THEN 'enqueued'
					WHEN status IN ('started', 'retried') THEN 'running'
					WHEN status = 'succeeded' THEN 'succeeded'
					WHEN status = 'cancelled' THEN 'cancelled'
					ELSE 'failed'
				END,
				COALESCE(created_at_value, NOW()),
				COALESCE(updated_at_value, NOW()),
				NULL, attempt_count, dead_lettered_value
			FROM ranked WHERE row_number = 1
			ON CONFLICT (message_id) DO NOTHING
		""")

	_RUNS_COLS = (
		"id, message_id, attempt, task, queue, instance_id, "
		"worker_pid, args, kwargs, headers, input_complete, result_preview, "
		"started_at, finished_at, "
		"time_elapsed, status, exc_type, exc_message, traceback, failure, state_override"
	)

	async def write_runs(self, runs: list[RunRow]) -> None:
		if not runs:
			return
		assert self._pool is not None
		lrt = self._qualified_logical_runs
		async with self._pool.acquire() as conn:
			await conn.executemany(
				f"""
					INSERT INTO {self._qualified_runs} AS current (
						message_id, attempt, task, queue, instance_id, worker_pid,
						args, kwargs, headers, input_complete, result_preview,
						started_at, finished_at, time_elapsed, status, exc_type,
						exc_message, traceback, failure, state_override
					) SELECT
					  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
					WHERE NOT EXISTS (
						SELECT 1 FROM {lrt}
						WHERE message_id = $1
							AND status IN ('succeeded', 'failed', 'cancelled')
					)
					ON CONFLICT (message_id, attempt) DO UPDATE SET
						task = EXCLUDED.task,
						queue = EXCLUDED.queue,
						instance_id = EXCLUDED.instance_id,
						worker_pid = EXCLUDED.worker_pid,
						args = EXCLUDED.args,
						kwargs = EXCLUDED.kwargs,
						headers = EXCLUDED.headers,
						input_complete = EXCLUDED.input_complete,
						result_preview = EXCLUDED.result_preview,
						started_at = EXCLUDED.started_at,
						finished_at = EXCLUDED.finished_at,
						time_elapsed = EXCLUDED.time_elapsed,
						status = EXCLUDED.status,
						exc_type = EXCLUDED.exc_type,
						exc_message = EXCLUDED.exc_message,
						traceback = EXCLUDED.traceback,
						failure = EXCLUDED.failure,
						state_override = CASE
							WHEN EXCLUDED.status IN ('succeeded', 'dead', 'cancelled') THEN NULL
							ELSE COALESCE(EXCLUDED.state_override, current.state_override) END
				""",
				[r.astuple() for r in runs],
			)

	async def write_logical_runs(self, runs: list[LogicalRunRow]) -> None:
		if not runs:
			return
		assert self._pool is not None
		lrt = self._qualified_logical_runs
		async with self._pool.acquire() as conn:
			await conn.executemany(
				f"""INSERT INTO {lrt} AS current (
					message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (message_id) DO UPDATE SET
					task = EXCLUDED.task,
					queue = EXCLUDED.queue,
					instance_id = EXCLUDED.instance_id,
					status = CASE
						WHEN current.status = 'cancel_requested'
							AND EXCLUDED.status = 'running' THEN current.status
						ELSE EXCLUDED.status END,
					created_at = LEAST(current.created_at, EXCLUDED.created_at),
					updated_at = GREATEST(current.updated_at, EXCLUDED.updated_at),
					replay_of = COALESCE(current.replay_of, EXCLUDED.replay_of),
					attempt_count = GREATEST(current.attempt_count, EXCLUDED.attempt_count),
					dead_lettered = current.dead_lettered OR EXCLUDED.dead_lettered
				WHERE current.status NOT IN ('succeeded', 'failed', 'cancelled')""",
				[r.astuple() for r in runs],
			)

	async def get_logical_run(self, message_id: str) -> LogicalRunRow | None:
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			row = await conn.fetchrow(
				f"""SELECT message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
					FROM {self._qualified_logical_runs} WHERE message_id = $1""",
				message_id,
			)
		return LogicalRunRow.fromrecord(row) if row is not None else None

	async def query_dashboard_stats(self, since: datetime) -> DashboardStats:
		assert self._pool is not None
		lrt = self._qualified_logical_runs
		since_naive = to_naive(since)
		async with self._pool.acquire() as conn:
			row = await conn.fetchrow(
				f"""SELECT
					COUNT(*) FILTER (WHERE created_at >= $1) AS enqueued,
					COUNT(*) FILTER (WHERE status = 'succeeded' AND updated_at >= $1) AS succeeded,
					COUNT(*) FILTER (WHERE status = 'failed' AND updated_at >= $1) AS failed,
					COALESCE(SUM(GREATEST(attempt_count - 1, 0))
						FILTER (WHERE created_at >= $1), 0) AS retried,
					COUNT(*) FILTER (WHERE dead_lettered AND updated_at >= $1) AS dead
					FROM {lrt}""",
				since_naive,
			)
		return DashboardStats(
			totals={
				name: int(row[name])
				for name in ("enqueued", "succeeded", "failed", "retried", "dead")
			}
		)

	async def query_logical_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		queue: str | None = None,
		search: str | None = None,
		before: datetime | None = None,
		after: datetime | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[LogicalRunRow]:
		assert self._pool is not None
		where: list[str] = []
		params: list[Any] = []
		if task is not None:
			params.append(task)
			where.append(f"POSITION(lower(${len(params)}) IN lower(task)) > 0")
		for value, column, operator in (
			(status, "status", "="),
			(queue, "queue", "="),
			(to_naive(before), "updated_at", "<="),
			(to_naive(after), "updated_at", ">="),
		):
			if value is not None:
				params.append(value)
				where.append(f"{column} {operator} ${len(params)}")
		if search is not None:
			params.append(search)
			position = len(params)
			where.append(
				f"""(POSITION(lower(${position}) IN lower(message_id)) > 0
					OR POSITION(lower(${position}) IN lower(task)) > 0
					OR EXISTS (SELECT 1 FROM {self._qualified_runs} AS attempt
						WHERE attempt.message_id = {self._qualified_logical_runs}.message_id
							AND POSITION(lower(${position}) IN lower(COALESCE(attempt.exc_message, ''))) > 0))"""
			)
		params.extend([limit, offset])
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""SELECT message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
					FROM {self._qualified_logical_runs}
					WHERE {" AND ".join(where) if where else "TRUE"}
					ORDER BY updated_at DESC
					LIMIT ${len(params) - 1} OFFSET ${len(params)}""",
				*params,
			)
		return [LogicalRunRow.fromrecord(row) for row in rows]

	async def query_attempts_for_runs(self, message_ids: list[str]) -> list[RunRow]:
		if not message_ids:
			return []
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""SELECT {self._RUNS_COLS} FROM {self._qualified_runs}
					WHERE message_id = ANY($1::text[])
					ORDER BY message_id, attempt""",
				message_ids,
			)
		return [RunRow.fromrecord(row) for row in rows]

	async def mark_cancel_requested(self, message_id: str, at: datetime) -> bool:
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			result = await conn.execute(
				f"""UPDATE {self._qualified_logical_runs}
					SET status = 'cancel_requested', updated_at = $2
					WHERE message_id = $1
					AND status NOT IN ('succeeded', 'failed', 'cancelled')""",
				message_id,
				to_naive(at),
			)
		return result != "UPDATE 0"

	async def mark_instance_unknown(self, instance_id: str, at: datetime) -> int:
		assert self._pool is not None
		async with self._pool.acquire() as conn, conn.transaction():
			rows = await conn.fetch(
				f"""UPDATE {self._qualified_runs} SET state_override = 'unknown'
					WHERE instance_id = $1 AND status = 'started' AND state_override IS NULL
					RETURNING message_id""",
				instance_id,
			)
			mids = list({row["message_id"] for row in rows})
			if mids:
				await conn.execute(
					f"""UPDATE {self._qualified_logical_runs}
						SET status = 'unknown', updated_at = $2
						WHERE message_id = ANY($1::text[])
						AND status NOT IN ('succeeded', 'failed', 'cancelled')""",
					mids,
					to_naive(at),
				)
		return len(rows)

	async def mark_previous_attempts_lost(self, message_id: str, attempt: int) -> int:
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			result = await conn.execute(
				f"""UPDATE {self._qualified_runs} SET state_override = 'lost'
					WHERE message_id = $1 AND attempt < $2 AND state_override = 'unknown' """,
				message_id,
				attempt,
			)
		return int(result.rsplit(" ", 1)[-1])

	async def write_logs(self, logs: list[LogRow]) -> None:
		if not logs:
			return
		assert self._pool is not None
		async with self._pool.acquire() as conn:
			await conn.executemany(
				f"""
				  INSERT INTO {self._qualified_logs}
				  (message_id, attempt, ts, kind, seq, level, logger, message,
				   fields, current, total, dropped)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
		after_id: int | None = None,
		after_dt: datetime | None = None,
		limit: int = 500,
	) -> list[LogRow]:
		assert self._pool is not None
		lt = self._qualified_logs

		where = "message_id = $2 AND attempt = $3"
		params: list = [message_id, attempt]
		idx = 4
		if after_id is not None:
			where += f" AND id > ${idx}"
			params.append(after_id)
			idx += 1
		if after_dt:
			where += f" AND ts > ${idx}"
			params.append(to_naive(after_dt))

		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""
				  SELECT id, message_id, attempt, ts, kind, seq, level, logger,
				         message, fields, current, total, dropped
					FROM {lt}
					WHERE {where}
					ORDER BY id ASC LIMIT $1
				""",
				limit,
				*params,
			)
		return [LogRow.fromrecord(r) for r in rows]

	async def prune(self, before_ts: datetime, max_runs: int | None = None) -> int:
		assert self._pool is not None
		rt = self._qualified_runs
		lrt = self._qualified_logical_runs
		lt = self._qualified_logs
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""SELECT DISTINCT ON (message_id)
						message_id, status, COALESCE(finished_at, started_at) AS last_seen
					FROM {rt}
					ORDER BY message_id, attempt DESC"""
			)
			terminal = sorted(
				(row for row in rows if row["status"] in {"succeeded", "dead", "cancelled"}),
				key=lambda row: row["last_seen"] or datetime.min,
				reverse=True,
			)
			cutoff = to_naive(before_ts)
			stale = [
				row["message_id"]
				for index, row in enumerate(terminal)
				if (row["last_seen"] is not None and row["last_seen"] < cutoff)
				or (max_runs is not None and index >= max_runs)
			]
			if not stale:
				return 0
			async with conn.transaction():
				await conn.execute(f"DELETE FROM {lt} WHERE message_id = ANY($1::text[])", stale)
				await conn.execute(f"DELETE FROM {rt} WHERE message_id = ANY($1::text[])", stale)
				await conn.execute(f"DELETE FROM {lrt} WHERE message_id = ANY($1::text[])", stale)
			return len(stale)
