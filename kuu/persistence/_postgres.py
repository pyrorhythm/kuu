from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from kuu.config import PersistenceConfig
from kuu.persistence._backend import PersistenceBackend
from kuu.persistence._rows import (
	LogRow,
	RunRow,
	validate_table_name,
)

if TYPE_CHECKING:
	from asyncpg import Pool

log = logging.getLogger("kuu.persistence.postgres")


def _parse_pg_dsn(dsn: str) -> str:
	if dsn.startswith("postgres://"):
		return "postgresql://" + dsn[len("postgres://") :]
	return dsn


class PostgresBackend(PersistenceBackend):
	def __init__(self, cfg: PersistenceConfig) -> None:
		self._cfg = cfg
		self._dsn = _parse_pg_dsn(cfg.dsn)
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
		import asyncpg

		pool = await asyncpg.create_pool(
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
			await conn.execute(f"""
				CREATE TABLE IF NOT EXISTS {rt} (
					id BIGINT PRIMARY KEY GENERATED ALWAYS BY IDENTITY,
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
					args_repr TEXT,
					kwargs_repr TEXT,
					started_at DOUBLE PRECISION,
					finished_at DOUBLE PRECISION,
					time_elapsed DOUBLE PRECISION,
					exc_type TEXT,
					exc_message TEXT,
					traceback TEXT
				);

				CREATE TABLE IF NOT EXISTS {lt} (
					id BIGINT PRIMARY KEY GENERATED ALWAYS BY IDENTITY,
					message_id TEXT NOT NULL,
					attempt INTEGER NOT NULL DEFAULT 0,
					ts DOUBLE PRECISION NOT NULL DEFAULT 0.0,
					level INTEGER NOT NULL DEFAULT 0,
					logger TEXT NOT NULL DEFAULT '',
					message TEXT NOT NULL DEFAULT ''
				)
			""")

			runs_uniq = f'"{self._cfg.runs_table}_message_id_attempt_uniq"'
			await conn.execute(
				f"CREATE UNIQUE INDEX IF NOT EXISTS {runs_uniq} ON {rt}(message_id, attempt)"
			)
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
		"worker_pid, args_repr, kwargs_repr, started_at, finished_at, "
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
						args_repr, kwargs_repr, started_at, finished_at,
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
				[
					(
						r.message_id,
						r.attempt,
						r.task,
						r.queue,
						r.instance_id,
						r.worker_pid,
						r.args_repr,
						r.kwargs_repr,
						r.started_at,
						r.finished_at,
						r.time_elapsed,
						r.status,
						r.exc_type,
						r.exc_message,
						r.traceback,
					)
					for r in runs
				],
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
				[
					(lr.message_id, lr.attempt, lr.ts, lr.level, lr.logger, lr.message)
					for lr in logs
				],
			)

	async def query_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		before: float | None = None,
		after: float | None = None,
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
			params.append(before)
			idx += 1
		if after is not None:
			where.append(f"finished_at >= ${idx}")
			params.append(after)
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
		return [_pg_row_to_run(r) for r in rows]

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
		return [_pg_row_to_run(r) for r in rows]

	async def query_logs(
		self, message_id: str, attempt: int, *, limit: int = 500, after_ts: float = 0.0
	) -> list[LogRow]:
		assert self._pool is not None
		lt = self._qualified_logs
		async with self._pool.acquire() as conn:
			rows = await conn.fetch(
				f"""
				  SELECT message_id, attempt, ts, level, logger, message
					FROM {lt}
					WHERE message_id = $1 AND attempt = $2 AND ts > $3
					ORDER BY ts ASC LIMIT $4
				""",
				message_id,
				attempt,
				after_ts,
				limit,
			)
		return [
			LogRow(
				message_id=r["message_id"],
				attempt=r["attempt"],
				ts=r["ts"],
				level=r["level"],
				logger=r["logger"],
				message=r["message"],
			)
			for r in rows
		]

	async def prune(self, before_ts: float) -> int:
		assert self._pool is not None
		rt = self._qualified_runs
		lt = self._qualified_logs
		async with self._pool.acquire() as conn:
			await conn.execute(
				f"""DELETE FROM {lt}
					WHERE (message_id, attempt) IN
					(SELECT message_id, attempt FROM {rt} WHERE finished_at < $1)""",
				before_ts,
			)
			result = await conn.execute(
				f"DELETE FROM {rt} WHERE finished_at < $1",
				before_ts,
			)
		parts = result.split()
		return int(parts[1]) if len(parts) > 1 else 0


def _pg_row_to_run(row: Any) -> RunRow:
	return RunRow(
		id=row["id"],
		message_id=row["message_id"],
		attempt=row["attempt"],
		task=row["task"],
		queue=row["queue"],
		instance_id=row["instance_id"],
		worker_pid=row["worker_pid"],
		args_repr=row["args_repr"],
		kwargs_repr=row["kwargs_repr"],
		started_at=row["started_at"],
		finished_at=row["finished_at"],
		time_elapsed=row["time_elapsed"],
		status=row["status"],
		exc_type=row["exc_type"],
		exc_message=row["exc_message"],
		traceback=row["traceback"],
	)
