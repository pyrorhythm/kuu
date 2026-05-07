from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import anyio.to_thread

from kuu.config import PersistenceConfig
from kuu.marshal import marshal as _m
from kuu.persistence._backend import PersistenceBackend
from kuu.persistence._rows import (
	LogRow,
	RunRow,
	to_naive,
	validate_table_name,
)

log = logging.getLogger("kuu.persistence.sqlite")


def _safe_json_text(value: Any) -> str | None:
	"""Serialize a value to a JSON string for TEXT-column storage."""
	if value is None:
		return None
	if isinstance(value, str):
		return value
	try:
		return json.dumps(value, default=str)
	except Exception:
		return str(value)


def _parse_json_text(value: str | None) -> Any:
	"""Parse a JSON string back to a Python object; falls back to raw string."""
	if value is None:
		return None
	try:
		return json.loads(value)
	except (json.JSONDecodeError, TypeError):
		return value


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
		self._conn: sqlite3.Connection
		self._initialized = False
		self._lock = threading.Lock()

	async def connect(self) -> None:
		await anyio.to_thread.run_sync(self._connect_sync)

	def _connect_sync(self) -> None:
		with self._lock:
			conn = sqlite3.connect(self._path, check_same_thread=False)
			conn.execute("PRAGMA journal_mode=WAL")
			conn.execute("PRAGMA synchronous=NORMAL")
			conn.execute("PRAGMA temp_store=MEMORY")
			self._conn = conn
			self._initialized = True

	async def close(self) -> None:
		await anyio.to_thread.run_sync(self._close_sync)

	def _close_sync(self) -> None:
		with self._lock:
			if self._conn is not None:
				self._conn.close()
				self._initialized = False

	async def init_schema(self) -> None:
		await anyio.to_thread.run_sync(self._init_schema_sync)

	def _init_schema_sync(self) -> None:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		lt = self._cfg.logs_table
		with self._lock:
			self._conn.executescript(f"""
			CREATE TABLE IF NOT EXISTS "{rt}" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				message_id TEXT NOT NULL,
				attempt INTEGER NOT NULL DEFAULT 0,
				task TEXT NOT NULL,
				queue TEXT NOT NULL,
				instance_id TEXT NOT NULL,
				worker_pid INTEGER NOT NULL DEFAULT 0,
				args JSONB,
				kwargs JSONB,
				started_at INTEGER,
				finished_at INTEGER,
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
			);""")

			self._conn.executescript(f"""
				CREATE TABLE IF NOT EXISTS "{lt}" (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					message_id TEXT NOT NULL,
					attempt INTEGER NOT NULL DEFAULT 0,
					ts INTEGER NOT NULL DEFAULT 0,
					level INTEGER NOT NULL DEFAULT 0,
					logger TEXT NOT NULL DEFAULT '',
				  message TEXT NOT NULL DEFAULT ''
				);
				""")

			self._conn.executescript(f"""
			CREATE UNIQUE INDEX IF NOT EXISTS "{rt}_message_id_attempt_uniq"
				ON "{rt}"(message_id, attempt);
			CREATE INDEX IF NOT EXISTS "{rt}_finished_at_idx"
				ON "{rt}"(finished_at DESC);
			CREATE INDEX IF NOT EXISTS "{rt}_task_status_idx"
				ON "{rt}"(task, status, finished_at DESC);
			CREATE INDEX IF NOT EXISTS "{lt}_message_id_attempt_ts_idx"
				ON "{lt}"(message_id, attempt, ts);
			""")
			self._conn.commit()

	async def write_runs(self, runs: list[RunRow]) -> None:
		if not runs:
			return
		await anyio.to_thread.run_sync(self._write_runs_sync, runs)

	def _write_runs_sync(self, runs: list[RunRow]) -> None:
		if not self._initialized:
			self._connect_sync()
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
					_m.json_encode(r.args),
					_m.json_encode(r.kwargs),
					int(r.started_at.timestamp()) if r.started_at else None,
					int(r.finished_at.timestamp()) if r.finished_at else None,
					r.time_elapsed.total_seconds() if r.time_elapsed else None,
					r.status,
					r.exc_type,
					r.exc_message,
					r.traceback,
				)
			)
		with self._lock:
			self._conn.executemany(
				f"""
					INSERT INTO "{rt}" (
						message_id, attempt, task, queue, instance_id, worker_pid,
						args, kwargs, started_at, finished_at,
						time_elapsed, status, exc_type, exc_message, traceback
					) VALUES (
						?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
					)
					ON CONFLICT(message_id, attempt) DO UPDATE SET
						finished_at = excluded.finished_at,
						time_elapsed = excluded.time_elapsed,
						status = excluded.status,
						exc_type = excluded.exc_type,
						exc_message = excluded.exc_message,
						traceback = excluded.traceback
				""",
				rows,
			)
			self._conn.commit()

	async def write_logs(self, logs: list[LogRow]) -> None:
		if not logs:
			return
		await anyio.to_thread.run_sync(self._write_logs_sync, logs)

	def _write_logs_sync(self, logs: list[LogRow]) -> None:
		if not self._initialized:
			self._connect_sync()
		lt = self._cfg.logs_table
		rows: list[tuple[Any, ...]] = []
		for lr in logs:
			nvts: datetime = to_naive(lr.ts)  # type:ignore
			rows.append(
				(lr.message_id, lr.attempt, int(nvts.timestamp()), lr.level, lr.logger, lr.message)
			)
		with self._lock:
			self._conn.executemany(
				f"""
					INSERT INTO "{lt}" (
						message_id, attempt, ts, level, logger, message
					) VALUES (
						?, ?, ?, ?, ?, ?
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
		before: datetime | None = None,
		after: datetime | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[RunRow]:
		return await anyio.to_thread.run_sync(
			self._query_runs_sync,
			task,
			status,
			int(before.timestamp()) if before is not None else None,
			int(after.timestamp()) if after is not None else None,
			limit,
			offset,
		)

	def _query_runs_sync(
		self,
		task: str | None,
		status: str | None,
		before: int | None,
		after: int | None,
		limit: int,
		offset: int,
	) -> list[RunRow]:
		if not self._initialized:
			self._connect_sync()
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
		with self._lock:
			cursor = self._conn.execute(
				f"""
						SELECT
							id, message_id, attempt, task, queue, instance_id,
							worker_pid, args, kwargs, started_at, finished_at,
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
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		with self._lock:
			cursor = self._conn.execute(
				f"""
					SELECT
						id, message_id, attempt, task, queue, instance_id,
						worker_pid, args, kwargs, started_at, finished_at,
						time_elapsed, status, exc_type, exc_message, traceback
					FROM "{rt}" WHERE message_id = ?
					ORDER BY attempt ASC
				""",
				(message_id,),
			)
			return [_row_to_run(row) for row in cursor.fetchall()]

	async def query_logs(
		self, message_id: str, attempt: int, *, after_dt: datetime | None = None, limit: int = 500
	) -> list[LogRow]:
		return await anyio.to_thread.run_sync(
			self._query_logs_sync, message_id, attempt, limit, after_dt
		)

	def _query_logs_sync(
		self,
		message_id: str,
		attempt: int,
		limit: int,
		after_dt: datetime | None,
	) -> list[LogRow]:
		if not self._initialized:
			self._connect_sync()
		lt = self._cfg.logs_table

		where = "message_id = ? AND attempt = ?"
		params: list = [message_id, attempt]
		if after_dt:
			where += " AND ts > ?"
			params.append(int(to_naive(after_dt).timestamp()))

		with self._lock:
			cursor = self._conn.execute(
				f"""SELECT message_id, attempt, ts, level, logger, message
					FROM "{lt}" WHERE {where}
					ORDER BY ts ASC LIMIT ?""",
				(*params, limit),
			)
			return [
				LogRow(
					message_id=row[0],
					attempt=row[1],
					ts=datetime.fromtimestamp(row[2], tz=timezone.utc),
					level=row[3],
					logger=row[4],
					message=row[5],
				)
				for row in cursor.fetchall()
			]

	async def prune(self, before_ts: datetime) -> int:
		return await anyio.to_thread.run_sync(self._prune_sync, int(before_ts.timestamp()))

	def _prune_sync(self, before_ts: int) -> int:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		lt = self._cfg.logs_table
		with self._lock:
			self._conn.execute(
				f"""DELETE FROM "{lt}"
					WHERE (message_id, attempt) IN
					(SELECT message_id, attempt FROM "{rt}" WHERE finished_at < ?)""",
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
		args=_m.json_decode(row[7]),
		kwargs=_m.json_decode(row[8]),
		started_at=datetime.fromtimestamp(row[9], tz=timezone.utc) if row[9] is not None else None,
		finished_at=datetime.fromtimestamp(row[10], tz=timezone.utc)
		if row[10] is not None
		else None,
		time_elapsed=timedelta(seconds=row[11]) if row[11] is not None else None,
		status=row[12],
		exc_type=row[13],
		exc_message=row[14],
		traceback=row[15],
	)
