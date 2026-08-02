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
	DashboardStats,
	LogRow,
	LogicalRunRow,
	RunRow,
	to_naive,
	validate_table_name,
)
from kuu.result import RemoteFailure, sanitize_remote_failure

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
		validate_table_name(cfg.logical_runs_table)
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
		lrt = self._cfg.logical_runs_table
		lt = self._cfg.logs_table
		with self._lock:
			self._conn.executescript(f"""
			CREATE TABLE IF NOT EXISTS "{lrt}" (
				message_id TEXT PRIMARY KEY,
				task TEXT NOT NULL,
				queue TEXT NOT NULL,
				instance_id TEXT NOT NULL,
				status TEXT NOT NULL CHECK (status IN (
					'enqueued', 'running', 'cancel_requested',
					'succeeded', 'failed', 'cancelled', 'unknown'
				)),
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				replay_of TEXT,
				attempt_count INTEGER NOT NULL DEFAULT 1,
				dead_lettered INTEGER NOT NULL DEFAULT 0
			);
			CREATE INDEX IF NOT EXISTS "{lrt}_updated_at_idx"
				ON "{lrt}"(updated_at DESC);
			""")

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
				headers JSONB,
				input_complete INTEGER NOT NULL DEFAULT 0,
				result_preview JSONB,
				started_at INTEGER,
				finished_at INTEGER,
			 	time_elapsed REAL,
				status TEXT NOT NULL DEFAULT 'succeeded'
					CHECK (status IN (
						'enqueued', 'started',
						'succeeded', 'failed',
					  'retried', 'dead', 'cancelled'
					)),
				exc_type TEXT,
				exc_message TEXT,
				traceback TEXT,
				failure JSONB,
				state_override TEXT CHECK (state_override IN ('unknown', 'lost'))
			);""")

			self._conn.executescript(f"""
				CREATE TABLE IF NOT EXISTS "{lt}" (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					message_id TEXT NOT NULL,
					attempt INTEGER NOT NULL DEFAULT 0,
					ts INTEGER NOT NULL DEFAULT 0,
					kind TEXT NOT NULL DEFAULT 'log',
					seq INTEGER NOT NULL DEFAULT 0,
					level INTEGER NOT NULL DEFAULT 0,
					logger TEXT NOT NULL DEFAULT '',
					message TEXT NOT NULL DEFAULT '',
					fields JSONB,
					current REAL,
					total REAL,
					dropped INTEGER NOT NULL DEFAULT 0
				);
				""")

			run_columns = {row[1] for row in self._conn.execute(f'PRAGMA table_info("{rt}")')}
			for name, ddl in {
				"headers": "JSONB",
				"input_complete": "INTEGER NOT NULL DEFAULT 0",
				"result_preview": "JSONB",
				"failure": "JSONB",
				"state_override": "TEXT",
			}.items():
				if name not in run_columns:
					self._conn.execute(f'ALTER TABLE "{rt}" ADD COLUMN "{name}" {ddl}')
			self._migrate_run_status_constraint(rt)

			columns = {row[1] for row in self._conn.execute(f'PRAGMA table_info("{lt}")')}
			for name, ddl in {
				"kind": "TEXT NOT NULL DEFAULT 'log'",
				"seq": "INTEGER NOT NULL DEFAULT 0",
				"fields": "JSONB",
				"current": "REAL",
				"total": "REAL",
				"dropped": "INTEGER NOT NULL DEFAULT 0",
			}.items():
				if name not in columns:
					self._conn.execute(f'ALTER TABLE "{lt}" ADD COLUMN "{name}" {ddl}')

			self._backfill_logical_runs(rt, lrt)
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

	def _migrate_run_status_constraint(self, rt: str) -> None:
		row = self._conn.execute(
			"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (rt,)
		).fetchone()
		if row is None or "'cancelled'" in (row[0] or ""):
			return
		tmp = f"{rt}__v2"
		columns = (
			"id, message_id, attempt, task, queue, instance_id, worker_pid, "
			"args, kwargs, headers, input_complete, result_preview, started_at, "
			"finished_at, time_elapsed, status, exc_type, exc_message, traceback, "
			"failure, state_override"
		)
		self._conn.commit()
		self._conn.execute("PRAGMA foreign_keys=OFF")
		try:
			self._conn.executescript(f'''
				DROP TABLE IF EXISTS "{tmp}";
				CREATE TABLE "{tmp}" (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					message_id TEXT NOT NULL,
					attempt INTEGER NOT NULL DEFAULT 0,
					task TEXT NOT NULL,
					queue TEXT NOT NULL,
					instance_id TEXT NOT NULL,
					worker_pid INTEGER NOT NULL DEFAULT 0,
					args JSONB,
					kwargs JSONB,
					headers JSONB,
					input_complete INTEGER NOT NULL DEFAULT 0,
					result_preview JSONB,
					started_at INTEGER,
					finished_at INTEGER,
					time_elapsed REAL,
					status TEXT NOT NULL DEFAULT 'succeeded' CHECK (status IN (
						'enqueued', 'started', 'succeeded', 'failed',
						'retried', 'dead', 'cancelled'
					)),
					exc_type TEXT,
					exc_message TEXT,
					traceback TEXT,
					failure JSONB,
					state_override TEXT CHECK (state_override IN ('unknown', 'lost'))
				);
				INSERT INTO "{tmp}" ({columns}) SELECT {columns} FROM "{rt}";
				DROP TABLE "{rt}";
				ALTER TABLE "{tmp}" RENAME TO "{rt}";
			''')
		finally:
			self._conn.execute("PRAGMA foreign_keys=ON")

	def _backfill_logical_runs(self, rt: str, lrt: str) -> None:
		self._conn.execute(f'''
			INSERT OR IGNORE INTO "{lrt}" (
				message_id, task, queue, instance_id, status, created_at,
				updated_at, replay_of, attempt_count, dead_lettered
			)
			SELECT latest.message_id, latest.task, latest.queue, latest.instance_id,
				CASE
					WHEN latest.state_override IN ('unknown', 'lost') THEN 'unknown'
					WHEN latest.status = 'enqueued' THEN 'enqueued'
					WHEN latest.status IN ('started', 'retried') THEN 'running'
					WHEN latest.status = 'succeeded' THEN 'succeeded'
					WHEN latest.status = 'cancelled' THEN 'cancelled'
					ELSE 'failed'
				END,
				COALESCE(agg.created_at, CAST(strftime('%s', 'now') AS INTEGER)),
				COALESCE(agg.updated_at, CAST(strftime('%s', 'now') AS INTEGER)),
				NULL, agg.attempt_count, agg.dead_lettered
			FROM "{rt}" AS latest
			JOIN (
				SELECT message_id,
					MAX(attempt) + 1 AS attempt_count,
					MIN(COALESCE(started_at, finished_at)) AS created_at,
					MAX(COALESCE(finished_at, started_at)) AS updated_at,
					MAX(CASE WHEN status = 'dead' THEN 1 ELSE 0 END) AS dead_lettered
				FROM "{rt}" GROUP BY message_id
			) AS agg ON agg.message_id = latest.message_id
			WHERE latest.id = (
				SELECT candidate.id FROM "{rt}" AS candidate
				WHERE candidate.message_id = latest.message_id
				ORDER BY candidate.attempt DESC, candidate.id DESC LIMIT 1
			)
		''')

	async def write_runs(self, runs: list[RunRow]) -> None:
		if not runs:
			return
		await anyio.to_thread.run_sync(self._write_runs_sync, runs)

	def _write_runs_sync(self, runs: list[RunRow]) -> None:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		lrt = self._cfg.logical_runs_table
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
					_m.json_encode(r.headers),
					int(r.input_complete),
					_m.json_encode(r.result_preview),
					int(r.started_at.timestamp()) if r.started_at else None,
					int(r.finished_at.timestamp()) if r.finished_at else None,
					r.time_elapsed.total_seconds() if r.time_elapsed else None,
					r.status,
					r.exc_type,
					r.exc_message,
					r.traceback,
					_m.json_encode(r.failure) if r.failure is not None else None,
					r.state_override,
					r.message_id,
				)
			)
		with self._lock:
			self._conn.executemany(
				f"""
					INSERT INTO "{rt}" (
						message_id, attempt, task, queue, instance_id, worker_pid,
						args, kwargs, headers, input_complete, result_preview,
						started_at, finished_at, time_elapsed, status, exc_type,
						exc_message, traceback, failure, state_override
					) SELECT
						?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
					WHERE NOT EXISTS (
						SELECT 1 FROM "{lrt}"
						WHERE message_id = ?
							AND status IN ('succeeded', 'failed', 'cancelled')
					)
					ON CONFLICT(message_id, attempt) DO UPDATE SET
						task = excluded.task,
						queue = excluded.queue,
						instance_id = excluded.instance_id,
						worker_pid = excluded.worker_pid,
						args = excluded.args,
						kwargs = excluded.kwargs,
						headers = excluded.headers,
						input_complete = excluded.input_complete,
						result_preview = excluded.result_preview,
						started_at = excluded.started_at,
						finished_at = excluded.finished_at,
						time_elapsed = excluded.time_elapsed,
						status = excluded.status,
						exc_type = excluded.exc_type,
						exc_message = excluded.exc_message,
						traceback = excluded.traceback,
						failure = excluded.failure,
						state_override = CASE
							WHEN excluded.status IN ('succeeded', 'dead', 'cancelled') THEN NULL
							ELSE COALESCE(excluded.state_override, state_override) END
				""",
				rows,
			)
			self._conn.commit()

	async def write_logical_runs(self, runs: list[LogicalRunRow]) -> None:
		if not runs:
			return
		await anyio.to_thread.run_sync(self._write_logical_runs_sync, runs)

	def _write_logical_runs_sync(self, runs: list[LogicalRunRow]) -> None:
		if not self._initialized:
			self._connect_sync()
		lrt = self._cfg.logical_runs_table
		rows = [
			(
				r.message_id,
				r.task,
				r.queue,
				r.instance_id,
				r.status,
				int(r.created_at.timestamp()),
				int(r.updated_at.timestamp()),
				r.replay_of,
				r.attempt_count,
				int(r.dead_lettered),
			)
			for r in runs
		]
		with self._lock:
			self._conn.executemany(
				f'''INSERT INTO "{lrt}" (
					message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(message_id) DO UPDATE SET
					task = excluded.task,
					queue = excluded.queue,
					instance_id = excluded.instance_id,
					status = CASE
						WHEN "{lrt}".status = 'cancel_requested'
							AND excluded.status = 'running' THEN "{lrt}".status
						ELSE excluded.status END,
					created_at = MIN("{lrt}".created_at, excluded.created_at),
					updated_at = MAX("{lrt}".updated_at, excluded.updated_at),
					replay_of = COALESCE("{lrt}".replay_of, excluded.replay_of),
					attempt_count = MAX("{lrt}".attempt_count, excluded.attempt_count),
					dead_lettered = MAX("{lrt}".dead_lettered, excluded.dead_lettered)
				WHERE "{lrt}".status NOT IN ('succeeded', 'failed', 'cancelled')''',
				rows,
			)
			self._conn.commit()

	async def get_logical_run(self, message_id: str) -> LogicalRunRow | None:
		return await anyio.to_thread.run_sync(self._get_logical_run_sync, message_id)

	def _get_logical_run_sync(self, message_id: str) -> LogicalRunRow | None:
		if not self._initialized:
			self._connect_sync()
		lrt = self._cfg.logical_runs_table
		with self._lock:
			row = self._conn.execute(
				f'''SELECT message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
					FROM "{lrt}" WHERE message_id = ?''',
				(message_id,),
			).fetchone()
		if row is None:
			return None
		return LogicalRunRow(
			message_id=row[0],
			task=row[1],
			queue=row[2],
			instance_id=row[3],
			status=row[4],
			created_at=datetime.fromtimestamp(row[5], tz=timezone.utc),
			updated_at=datetime.fromtimestamp(row[6], tz=timezone.utc),
			replay_of=row[7],
			attempt_count=row[8],
			dead_lettered=bool(row[9]),
		)

	async def query_dashboard_stats(self, since: datetime) -> DashboardStats:
		return await anyio.to_thread.run_sync(
			self._query_dashboard_stats_sync, int(since.timestamp())
		)

	def _query_dashboard_stats_sync(self, since: int) -> DashboardStats:
		if not self._initialized:
			self._connect_sync()
		lrt = self._cfg.logical_runs_table
		with self._lock:
			row = self._conn.execute(
				f'''SELECT
					SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END),
					SUM(CASE WHEN status = 'succeeded' AND updated_at >= ? THEN 1 ELSE 0 END),
					SUM(CASE WHEN status = 'failed' AND updated_at >= ? THEN 1 ELSE 0 END),
					SUM(CASE WHEN created_at >= ? THEN MAX(attempt_count - 1, 0) ELSE 0 END),
					SUM(CASE WHEN dead_lettered = 1 AND updated_at >= ? THEN 1 ELSE 0 END)
					FROM "{lrt}"''',
				(since, since, since, since, since),
			).fetchone()
		return DashboardStats(
			totals=dict(
				zip(
					("enqueued", "succeeded", "failed", "retried", "dead"),
					(int(value or 0) for value in row),
					strict=True,
				)
			)
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
		return await anyio.to_thread.run_sync(
			self._query_logical_runs_sync,
			task,
			status,
			queue,
			search,
			int(before.timestamp()) if before is not None else None,
			int(after.timestamp()) if after is not None else None,
			limit,
			offset,
		)

	def _query_logical_runs_sync(
		self,
		task: str | None,
		status: str | None,
		queue: str | None,
		search: str | None,
		before: int | None,
		after: int | None,
		limit: int,
		offset: int,
	) -> list[LogicalRunRow]:
		if not self._initialized:
			self._connect_sync()
		lrt = self._cfg.logical_runs_table
		rt = self._cfg.runs_table
		where: list[str] = []
		params: list[Any] = []
		if task is not None:
			where.append("instr(lower(task), lower(?)) > 0")
			params.append(task)
		for value, clause in (
			(status, "status = ?"),
			(queue, "queue = ?"),
			(before, "updated_at <= ?"),
			(after, "updated_at >= ?"),
		):
			if value is not None:
				where.append(clause)
				params.append(value)
		if search is not None:
			where.append(
				f'''(instr(lower(message_id), lower(?)) > 0
					OR instr(lower(task), lower(?)) > 0
					OR EXISTS (SELECT 1 FROM "{rt}" AS attempt
						WHERE attempt.message_id = "{lrt}".message_id
							AND instr(lower(COALESCE(attempt.exc_message, '')), lower(?)) > 0))'''
			)
			params.extend([search, search, search])
		params.extend([limit, offset])
		with self._lock:
			rows = self._conn.execute(
				f'''SELECT message_id, task, queue, instance_id, status, created_at,
					updated_at, replay_of, attempt_count, dead_lettered
					FROM "{lrt}" WHERE {" AND ".join(where) if where else "1=1"}
					ORDER BY updated_at DESC LIMIT ? OFFSET ?''',
				params,
			).fetchall()
		return [_row_to_logical(row) for row in rows]

	async def query_attempts_for_runs(self, message_ids: list[str]) -> list[RunRow]:
		return await anyio.to_thread.run_sync(self._query_attempts_for_runs_sync, message_ids)

	def _query_attempts_for_runs_sync(self, message_ids: list[str]) -> list[RunRow]:
		if not message_ids:
			return []
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		placeholders = ",".join("?" for _ in message_ids)
		with self._lock:
			rows = self._conn.execute(
				f'''SELECT id, message_id, attempt, task, queue, instance_id,
					worker_pid, args, kwargs, headers, input_complete, result_preview,
					started_at, finished_at, time_elapsed,
					status, exc_type, exc_message, traceback, failure, state_override
					FROM "{rt}" WHERE message_id IN ({placeholders})
					ORDER BY message_id, attempt''',
				message_ids,
			).fetchall()
		return [_row_to_run(row) for row in rows]

	async def mark_cancel_requested(self, message_id: str, at: datetime) -> bool:
		return await anyio.to_thread.run_sync(
			self._mark_cancel_requested_sync, message_id, int(at.timestamp())
		)

	def _mark_cancel_requested_sync(self, message_id: str, at: int) -> bool:
		if not self._initialized:
			self._connect_sync()
		lrt = self._cfg.logical_runs_table
		with self._lock:
			cursor = self._conn.execute(
				f'''UPDATE "{lrt}" SET status = 'cancel_requested', updated_at = ?
					WHERE message_id = ? AND status NOT IN ('succeeded', 'failed', 'cancelled')''',
				(at, message_id),
			)
			self._conn.commit()
			return cursor.rowcount > 0

	async def mark_instance_unknown(self, instance_id: str, at: datetime) -> int:
		return await anyio.to_thread.run_sync(
			self._mark_instance_unknown_sync, instance_id, int(at.timestamp())
		)

	def _mark_instance_unknown_sync(self, instance_id: str, at: int) -> int:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		lrt = self._cfg.logical_runs_table
		with self._lock:
			mids = [
				row[0]
				for row in self._conn.execute(
					f'''SELECT DISTINCT message_id FROM "{rt}"
						WHERE instance_id = ? AND status = 'started' AND state_override IS NULL''',
					(instance_id,),
				).fetchall()
			]
			cursor = self._conn.execute(
				f'''UPDATE "{rt}" SET state_override = 'unknown'
					WHERE instance_id = ? AND status = 'started' AND state_override IS NULL''',
				(instance_id,),
			)
			if mids:
				placeholders = ",".join("?" for _ in mids)
				self._conn.execute(
					f'''UPDATE "{lrt}" SET status = 'unknown', updated_at = ?
						WHERE message_id IN ({placeholders})
						AND status NOT IN ('succeeded', 'failed', 'cancelled')''',
					(at, *mids),
				)
			self._conn.commit()
			return cursor.rowcount

	async def mark_previous_attempts_lost(self, message_id: str, attempt: int) -> int:
		return await anyio.to_thread.run_sync(
			self._mark_previous_attempts_lost_sync, message_id, attempt
		)

	def _mark_previous_attempts_lost_sync(self, message_id: str, attempt: int) -> int:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		with self._lock:
			cursor = self._conn.execute(
				f'''UPDATE "{rt}" SET state_override = 'lost'
					WHERE message_id = ? AND attempt < ? AND state_override = 'unknown' ''',
				(message_id, attempt),
			)
			self._conn.commit()
			return cursor.rowcount

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
				(
					lr.message_id,
					lr.attempt,
					int(nvts.timestamp()),
					lr.kind,
					lr.seq,
					lr.level,
					lr.logger,
					lr.message,
					_m.json_encode(lr.fields).decode(),
					lr.current,
					lr.total,
					lr.dropped,
				)
			)
		with self._lock:
			self._conn.executemany(
				f"""
					INSERT INTO "{lt}" (
						message_id, attempt, ts, kind, seq, level, logger, message,
						fields, current, total, dropped
					) VALUES (
						?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
							worker_pid, args, kwargs, headers, input_complete, result_preview,
							started_at, finished_at, time_elapsed,
							status, exc_type, exc_message, traceback, failure,
							state_override
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
						worker_pid, args, kwargs, headers, input_complete, result_preview,
						started_at, finished_at, time_elapsed,
						status, exc_type, exc_message, traceback, failure,
						state_override
					FROM "{rt}" WHERE message_id = ?
					ORDER BY attempt ASC
				""",
				(message_id,),
			)
			return [_row_to_run(row) for row in cursor.fetchall()]

	async def query_logs(
		self,
		message_id: str,
		attempt: int,
		*,
		after_id: int | None = None,
		after_dt: datetime | None = None,
		limit: int = 500,
	) -> list[LogRow]:
		return await anyio.to_thread.run_sync(
			self._query_logs_sync, message_id, attempt, limit, after_id, after_dt
		)

	def _query_logs_sync(
		self,
		message_id: str,
		attempt: int,
		limit: int,
		after_id: int | None,
		after_dt: datetime | None,
	) -> list[LogRow]:
		if not self._initialized:
			self._connect_sync()
		lt = self._cfg.logs_table

		where = "message_id = ? AND attempt = ?"
		params: list = [message_id, attempt]
		if after_id is not None:
			where += " AND id > ?"
			params.append(after_id)
		if after_dt:
			where += " AND ts > ?"
			params.append(int(to_naive(after_dt).timestamp()))

		with self._lock:
			cursor = self._conn.execute(
				f"""SELECT id, message_id, attempt, ts, kind, seq, level, logger,
						message, fields, current, total, dropped
					FROM "{lt}" WHERE {where}
					ORDER BY id ASC LIMIT ?""",
				(*params, limit),
			)
			return [
				LogRow(
					id=row[0],
					message_id=row[1],
					attempt=row[2],
					ts=datetime.fromtimestamp(row[3], tz=timezone.utc),
					kind=row[4],
					seq=row[5],
					level=row[6],
					logger=row[7],
					message=row[8],
					fields=_m.json_decode(row[9]) if row[9] else {},
					current=row[10],
					total=row[11],
					dropped=row[12],
				)
				for row in cursor.fetchall()
			]

	async def prune(self, before_ts: datetime, max_runs: int | None = None) -> int:
		return await anyio.to_thread.run_sync(
			self._prune_sync, int(before_ts.timestamp()), max_runs
		)

	def _prune_sync(self, before_ts: int, max_runs: int | None) -> int:
		if not self._initialized:
			self._connect_sync()
		rt = self._cfg.runs_table
		lrt = self._cfg.logical_runs_table
		lt = self._cfg.logs_table
		with self._lock:
			rows = self._conn.execute(
				f'''SELECT r.message_id, r.status,
						COALESCE(r.finished_at, r.started_at, 0) AS last_seen
					FROM "{rt}" AS r
					JOIN (
						SELECT message_id, MAX(attempt) AS attempt
						FROM "{rt}" GROUP BY message_id
					) AS latest
					ON latest.message_id = r.message_id AND latest.attempt = r.attempt'''
			).fetchall()
			terminal = sorted(
				(row for row in rows if row[1] in {"succeeded", "dead", "cancelled"}),
				key=lambda row: row[2],
				reverse=True,
			)
			stale = [
				row[0]
				for index, row in enumerate(terminal)
				if row[2] < before_ts or (max_runs is not None and index >= max_runs)
			]
			for offset in range(0, len(stale), 500):
				chunk = stale[offset : offset + 500]
				placeholders = ",".join("?" for _ in chunk)
				self._conn.execute(
					f'DELETE FROM "{lt}" WHERE message_id IN ({placeholders})', chunk
				)
				self._conn.execute(
					f'DELETE FROM "{rt}" WHERE message_id IN ({placeholders})', chunk
				)
				self._conn.execute(
					f'DELETE FROM "{lrt}" WHERE message_id IN ({placeholders})', chunk
				)
			self._conn.execute("PRAGMA optimize")
			self._conn.commit()
			return len(stale)


def _row_to_logical(row: tuple[Any, ...]) -> LogicalRunRow:
	return LogicalRunRow(
		message_id=row[0],
		task=row[1],
		queue=row[2],
		instance_id=row[3],
		status=row[4],
		created_at=datetime.fromtimestamp(row[5], tz=timezone.utc),
		updated_at=datetime.fromtimestamp(row[6], tz=timezone.utc),
		replay_of=row[7],
		attempt_count=row[8],
		dead_lettered=bool(row[9]),
	)


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
		headers=_m.json_decode(row[9]) if row[9] else None,
		input_complete=bool(row[10]),
		result_preview=_m.json_decode(row[11]) if row[11] else None,
		started_at=datetime.fromtimestamp(row[12], tz=timezone.utc)
		if row[12] is not None
		else None,
		finished_at=datetime.fromtimestamp(row[13], tz=timezone.utc)
		if row[13] is not None
		else None,
		time_elapsed=timedelta(seconds=row[14]) if row[14] is not None else None,
		status=row[15],
		exc_type=row[16],
		exc_message=row[17],
		traceback=row[18],
		failure=(
			sanitize_remote_failure(_m.json_decode(row[19], type=RemoteFailure))
			if row[19]
			else None
		),
		state_override=row[20],
	)
