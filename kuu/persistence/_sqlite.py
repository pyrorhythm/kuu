from __future__ import annotations

import logging
import sqlite3
from typing import Any

import anyio
import anyio.to_thread

from kuu.config import PersistenceConfig
from kuu.persistence._backend import PersistenceBackend
from kuu.persistence._rows import (
	LogRow,
	RunRow,
	validate_table_name,
)

log = logging.getLogger("kuu.persistence.sqlite")


def _parse_sqlite_dsn(dsn: str) -> str:
	if dsn.startswith("sqlite:///"):
		return dsn[len("sqlite:///") :]
	if dsn == "sqlite://":
		return ":memory:"
	if dsn.startswith("sqlite://"):
		return dsn[len("sqlite://") :]
	return dsn


class SqliteBackend(PersistenceBackend):
	def __init__(self, cfg: PersistenceConfig) -> None:
		self._cfg = cfg
		self._path = _parse_sqlite_dsn(cfg.dsn)
		validate_table_name(cfg.runs_table)
		validate_table_name(cfg.logs_table)
		self._conn: sqlite3.Connection | None = None

	async def connect(self) -> None:
		await anyio.to_thread.run_sync(self._connect_sync)

	def _connect_sync(self) -> None:
		conn = sqlite3.connect(self._path, check_same_thread=False)
		conn.execute("PRAGMA journal_mode=WAL")
		conn.execute("PRAGMA synchronous=NORMAL")
		conn.execute("PRAGMA temp_store=MEMORY")
		conn.execute("PRAGMA foreign_keys=ON")
		self._conn = conn

	async def close(self) -> None:
		await anyio.to_thread.run_sync(self._close_sync)

	def _close_sync(self) -> None:
		if self._conn is not None:
			self._conn.close()
			self._conn = None

	async def init_schema(self) -> None:
		await anyio.to_thread.run_sync(self._init_schema_sync)

	def _init_schema_sync(self) -> None:
		assert self._conn is not None
		rt = self._cfg.runs_table
		lt = self._cfg.logs_table
		self._conn.executescript(f"""
			CREATE TABLE IF NOT EXISTS "{rt}" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				message_id TEXT NOT NULL,
				attempt INTEGER NOT NULL DEFAULT 0,
				task TEXT NOT NULL,
				queue TEXT NOT NULL,
				instance_id TEXT NOT NULL,
				worker_pid INTEGER NOT NULL DEFAULT 0,
				args_repr TEXT,
				kwargs_repr TEXT,
				started_at REAL,
				finished_at REAL,
			 	time_elapsed REAL,
				status TEXT NOT NULL DEFAULT 'succeeded'
					CHECK (status IN (
						'enqueued', 'started',
						'succeeded', 'failed',
					  'retried', 'dead'
					)),
				exc_type TEXT,
				exc_message TEXT,
				traceback TEXT
			);
			CREATE TABLE IF NOT EXISTS "{lt}" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				run_id INTEGER NOT NULL,
				ts REAL NOT NULL DEFAULT 0.0,
				level INTEGER NOT NULL DEFAULT 0,
				logger TEXT NOT NULL DEFAULT '',
			  message TEXT NOT NULL DEFAULT '',
				FOREIGN KEY (run_id) REFERENCES "{rt}"(id) ON DELETE CASCADE
			);
			CREATE INDEX IF NOT EXISTS "{rt}_message_id_idx"
				ON "{rt}"(message_id, attempt);
			CREATE INDEX IF NOT EXISTS "{rt}_finished_at_idx"
				ON "{rt}"(finished_at DESC);
			CREATE INDEX IF NOT EXISTS "{rt}_task_status_idx"
				ON "{rt}"(task, status, finished_at DESC);
			CREATE INDEX IF NOT EXISTS "{lt}_run_id_ts_idx"
				ON "{lt}"(run_id, ts);
		""")
		self._conn.commit()

	async def write_runs(self, runs: list[RunRow]) -> None:
		if not runs:
			return
		await anyio.to_thread.run_sync(self._write_runs_sync, runs)

	def _write_runs_sync(self, runs: list[RunRow]) -> None:
		assert self._conn is not None
		rt = self._cfg.runs_table
		rows: list[tuple[Any, ...]] = []
		for r in runs:
			rows.append(
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
			)
		self._conn.executemany(
			f"""
				INSERT INTO "{rt}" (
					message_id, attempt, task, queue, instance_id, worker_pid,
					args_repr, kwargs_repr, started_at, finished_at,
					time_elapsed, status, exc_type, exc_message, traceback
				) VALUES (
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
				)
			""",
			rows,
		)
		self._conn.commit()

	async def write_logs(self, logs: list[LogRow]) -> None:
		if not logs:
			return
		await anyio.to_thread.run_sync(self._write_logs_sync, logs)

	def _write_logs_sync(self, logs: list[LogRow]) -> None:
		assert self._conn is not None
		lt = self._cfg.logs_table
		rows: list[tuple[Any, ...]] = []
		for lr in logs:
			rows.append((lr.run_id, lr.ts, lr.level, lr.logger, lr.message))
		self._conn.executemany(
			f"""
				INSERT INTO "{lt}" (
					run_id, ts, level, logger, message
				) VALUES (
					?, ?, ?, ?, ?
				)
			""",
			rows,
		)
		self._conn.commit()

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
		return await anyio.to_thread.run_sync(
			self._query_runs_sync, task, status, before, after, limit, offset
		)

	def _query_runs_sync(
		self,
		task: str | None,
		status: str | None,
		before: float | None,
		after: float | None,
		limit: int,
		offset: int,
	) -> list[RunRow]:
		assert self._conn is not None
		rt = self._cfg.runs_table
		where: list[str] = []
		params: list[Any] = []
		if task is not None:
			where.append("task = ?")
			params.append(task)
		if status is not None:
			where.append("status = ?")
			params.append(status)
		if before is not None:
			where.append("finished_at <= ?")
			params.append(before)
		if after is not None:
			where.append("finished_at >= ?")
			params.append(after)
		clause = " AND ".join(where) if where else "1=1"
		params.extend([limit, offset])
		cursor = self._conn.execute(
			f"""
					SELECT
						id, message_id, attempt, task, queue, instance_id,
						worker_pid, args_repr, kwargs_repr, started_at, finished_at,
						time_elapsed, status, exc_type, exc_message, traceback
					FROM "{rt}" WHERE {clause}
					ORDER BY finished_at DESC
					LIMIT ? OFFSET ?
			""",
			params,
		)
		return [_row_to_run(row) for row in cursor.fetchall()]

	async def query_run_attempts(self, message_id: str) -> list[RunRow]:
		return await anyio.to_thread.run_sync(self._query_run_attempts_sync, message_id)

	def _query_run_attempts_sync(self, message_id: str) -> list[RunRow]:
		assert self._conn is not None
		rt = self._cfg.runs_table
		cursor = self._conn.execute(
			f"""
				SELECT
					id, message_id, attempt, task, queue, instance_id,
					worker_pid, args_repr, kwargs_repr, started_at, finished_at,
					time_elapsed, status, exc_type, exc_message, traceback
				FROM "{rt}" WHERE message_id = ?
				ORDER BY attempt ASC
			""",
			(message_id,),
		)
		return [_row_to_run(row) for row in cursor.fetchall()]

	async def query_logs(
		self, run_id: int, *, limit: int = 500, after_ts: float = 0.0
	) -> list[LogRow]:
		return await anyio.to_thread.run_sync(self._query_logs_sync, run_id, limit, after_ts)

	def _query_logs_sync(self, run_id: int, limit: int, after_ts: float) -> list[LogRow]:
		assert self._conn is not None
		lt = self._cfg.logs_table
		cursor = self._conn.execute(
			f"""SELECT run_id, ts, level, logger, message
				FROM "{lt}" WHERE run_id = ? AND ts > ?
				ORDER BY ts ASC LIMIT ?""",
			(run_id, after_ts, limit),
		)
		return [
			LogRow(run_id=row[0], ts=row[1], level=row[2], logger=row[3], message=row[4])
			for row in cursor.fetchall()
		]

	async def prune(self, before_ts: float) -> int:
		return await anyio.to_thread.run_sync(self._prune_sync, before_ts)

	def _prune_sync(self, before_ts: float) -> int:
		assert self._conn is not None
		rt = self._cfg.runs_table
		lt = self._cfg.logs_table
		self._conn.execute(
			f"""DELETE FROM "{lt}" WHERE run_id IN
				(SELECT id FROM "{rt}" WHERE finished_at < ?)""",
			(before_ts,),
		)
		cursor = self._conn.execute(f'DELETE FROM "{rt}" WHERE finished_at < ?', (before_ts,))
		count = cursor.rowcount
		self._conn.execute("PRAGMA optimize")
		self._conn.commit()
		return count


def _row_to_run(row: tuple[Any, ...]) -> RunRow:
	return RunRow(
		id=row[0],
		message_id=row[1],
		attempt=row[2],
		task=row[3],
		queue=row[4],
		instance_id=row[5],
		worker_pid=row[6],
		args_repr=row[7],
		kwargs_repr=row[8],
		started_at=row[9],
		finished_at=row[10],
		time_elapsed=row[11],
		status=row[12],
		exc_type=row[13],
		exc_message=row[14],
		traceback=row[15],
	)
