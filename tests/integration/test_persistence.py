from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import pytest
from testcontainers.postgres import PostgresContainer

from kuu._util import utcnow
from kuu.config import PersistenceConfig
from kuu.observability._protocol import Event, LogBatch, LogRecord
from kuu.persistence import (
	NoopBackend,
	PersistenceBackend,
	PersistenceWorker,
	create_backend,
)
from kuu.persistence._postgres import PostgresBackend
from kuu.persistence._rows import LogRow, RunRow, validate_table_name
from kuu.persistence._sqlite import SqliteBackend, _parse_sqlite_dsn

pytestmark = pytest.mark.anyio


def _pg_dsn(container: PostgresContainer) -> str:
	host = container.get_container_host_ip()
	port = int(container.get_exposed_port(container.port))
	return (
		f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"
	)


def _uniq(prefix: str = "kuu") -> str:
	return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def sqlite_backend(tmp_path) -> AsyncIterator[SqliteBackend]:
	cfg = PersistenceConfig(
		enable=True,
		dsn=f"sqlite:///{tmp_path / 'p.db'}",
		runs_table=_uniq("runs"),
		logs_table=_uniq("logs"),
		keep_days=7,
	)
	be = SqliteBackend(cfg)
	await be.connect()
	await be.init_schema()
	yield be
	await be.close()


@pytest.fixture
async def postgres_backend(postgres_container: PostgresContainer) -> AsyncIterator[PostgresBackend]:
	schema = _uniq("psch")
	cfg = PersistenceConfig(
		enable=True,
		dsn=_pg_dsn(postgres_container),
		schema=schema,
		runs_table="runs",
		logs_table="logs",
		keep_days=7,
	)
	be = PostgresBackend(cfg)
	await be.connect()
	await be.init_schema()
	yield be
	try:
		assert be._pool is not None
		async with be._pool.acquire() as conn:
			await conn.execute(f'drop schema if exists "{schema}" cascade')
	finally:
		await be.close()


@pytest.fixture(params=["sqlite", "postgres"])
async def backend(
	request, tmp_path, postgres_container: PostgresContainer
) -> AsyncIterator[PersistenceBackend]:
	if request.param == "sqlite":
		cfg = PersistenceConfig(
			enable=True,
			dsn=f"sqlite:///{tmp_path / 'p.db'}",
			runs_table=_uniq("runs"),
			logs_table=_uniq("logs"),
		)
		be = SqliteBackend(cfg)
		await be.connect()
		await be.init_schema()
		yield be
		await be.close()
	else:
		schema = _uniq("psch")
		cfg = PersistenceConfig(
			enable=True,
			dsn=_pg_dsn(postgres_container),
			schema=schema,
			runs_table="runs",
			logs_table="logs",
		)
		pgbe = PostgresBackend(cfg)
		await pgbe.connect()
		await pgbe.init_schema()
		yield pgbe
		try:
			assert pgbe._pool is not None
			async with pgbe._pool.acquire() as conn:
				await conn.execute(f'drop schema if exists "{schema}" cascade')
		finally:
			await pgbe.close()


# === helpers


def _now() -> datetime:
	return utcnow().replace(microsecond=0)


def _run(
	*,
	mid: str,
	attempt: int = 1,
	task: str = "t",
	queue: str = "default",
	status: str = "succeeded",
	finished_at: datetime | None = None,
	started_at: datetime | None = None,
	exc_type: str | None = None,
	exc_message: str | None = None,
) -> RunRow:
	return RunRow(
		message_id=mid,
		attempt=attempt,
		task=task,
		queue=queue,
		instance_id="inst-1",
		worker_pid=42,
		started_at=started_at,
		finished_at=finished_at if finished_at is not None else _now(),
		time_elapsed=timedelta(seconds=0.05),
		status=status,  # type: ignore[arg-type]
		exc_type=exc_type,
		exc_message=exc_message,
	)


def _log(
	*,
	mid: str,
	attempt: int = 1,
	ts: datetime | None = None,
	level: int = 20,
	logger: str = "app",
	message: str = "hi",
) -> LogRow:
	return LogRow(
		message_id=mid,
		attempt=attempt,
		ts=ts if ts is not None else utcnow(),
		level=level,
		logger=logger,
		message=message,
	)


# === backend tests (run against both sqlite and postgres)


class TestBackend:
	async def test_init_schema_idempotent(self, backend: PersistenceBackend) -> None:
		# init_schema is called by fixture; calling again must not raise
		await backend.init_schema()
		await backend.init_schema()

	async def test_write_and_query_run(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		await backend.write_runs([_run(mid=mid, task="alpha")])
		got = await backend.query_runs(task="alpha")
		assert len(got) == 1
		assert got[0].message_id == mid
		assert got[0].task == "alpha"
		assert got[0].status == "succeeded"

	async def test_write_runs_empty_is_noop(self, backend: PersistenceBackend) -> None:
		await backend.write_runs([])
		assert await backend.query_runs() == []

	async def test_write_logs_empty_is_noop(self, backend: PersistenceBackend) -> None:
		await backend.write_logs([])

	async def test_upsert_replaces_finish_columns(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		t0 = _now()
		await backend.write_runs(
			[
				_run(
					mid=mid,
					attempt=1,
					status="failed",
					finished_at=t0,
					exc_type="ValueError",
					exc_message="bad",
				)
			]
		)
		await backend.write_runs(
			[_run(mid=mid, attempt=1, status="succeeded", finished_at=t0 + timedelta(seconds=1))]
		)
		attempts = await backend.query_run_attempts(mid)
		assert len(attempts) == 1, "upsert should not produce a duplicate row"
		assert attempts[0].status == "succeeded"
		assert attempts[0].exc_type is None or attempts[0].exc_type == ""
		assert attempts[0].finished_at is not None
		assert attempts[0].finished_at >= t0 + timedelta(seconds=0.5)

	async def test_query_run_attempts_orders_by_attempt(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		await backend.write_runs(
			[
				_run(mid=mid, attempt=2, status="succeeded"),
				_run(mid=mid, attempt=1, status="retried"),
				_run(mid=mid, attempt=3, status="succeeded"),
			]
		)
		got = await backend.query_run_attempts(mid)
		assert [r.attempt for r in got] == [1, 2, 3]
		assert got[0].status == "retried"

	async def test_query_runs_filters_by_status_and_task(self, backend: PersistenceBackend) -> None:
		await backend.write_runs(
			[
				_run(mid=_uniq("m"), task="a", status="succeeded"),
				_run(mid=_uniq("m"), task="a", status="failed"),
				_run(mid=_uniq("m"), task="b", status="failed"),
			]
		)
		failed_a = await backend.query_runs(task="a", status="failed")
		assert len(failed_a) == 1
		assert failed_a[0].task == "a" and failed_a[0].status == "failed"

	async def test_query_runs_time_window(self, backend: PersistenceBackend) -> None:
		now = _now()
		await backend.write_runs(
			[
				_run(mid=_uniq("old"), task="w", finished_at=now - timedelta(seconds=1000)),
				_run(mid=_uniq("mid"), task="w", finished_at=now - timedelta(seconds=100)),
				_run(mid=_uniq("new"), task="w", finished_at=now),
			]
		)
		got = await backend.query_runs(
			task="w", after=now - timedelta(seconds=500), before=now + timedelta(seconds=1)
		)
		assert len(got) == 2
		assert got[0].finished_at >= got[1].finished_at

	async def test_query_runs_pagination(self, backend: PersistenceBackend) -> None:
		now = _now()
		mids = [_uniq("p") for _ in range(5)]
		await backend.write_runs(
			[
				_run(mid=m, task="pg", finished_at=now - timedelta(seconds=i))
				for i, m in enumerate(mids)
			]
		)
		page1 = await backend.query_runs(task="pg", limit=2, offset=0)
		page2 = await backend.query_runs(task="pg", limit=2, offset=2)
		assert len(page1) == 2 and len(page2) == 2
		assert {r.message_id for r in page1}.isdisjoint({r.message_id for r in page2})

	async def test_logs_round_trip(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		# write runs first so FK constraint on logs is satisfied
		await backend.write_runs([_run(mid=mid, attempt=1), _run(mid=mid, attempt=2)])
		t0 = time.time()
		await backend.write_logs(
			[
				_log(
					mid=mid,
					attempt=1,
					ts=datetime.fromtimestamp(t0 + 1, tz=timezone.utc),
					message="first",
				),
				_log(
					mid=mid,
					attempt=1,
					ts=datetime.fromtimestamp(t0 + 2, tz=timezone.utc),
					message="second",
				),
				_log(
					mid=mid,
					attempt=2,
					ts=datetime.fromtimestamp(t0 + 3, tz=timezone.utc),
					message="other-attempt",
				),
			]
		)
		a1 = await backend.query_logs(mid, 1)
		assert [r.message for r in a1] == ["first", "second"]
		a2 = await backend.query_logs(mid, 2)
		assert [r.message for r in a2] == ["other-attempt"]

	async def test_query_logs_after_ts_cursor(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		await backend.write_runs([_run(mid=mid, attempt=1)])
		t0 = time.time()
		await backend.write_logs(
			[
				_log(mid=mid, ts=datetime.fromtimestamp(t0 + 1, tz=timezone.utc), message="a"),
				_log(mid=mid, ts=datetime.fromtimestamp(t0 + 2, tz=timezone.utc), message="b"),
				_log(mid=mid, ts=datetime.fromtimestamp(t0 + 3, tz=timezone.utc), message="c"),
			]
		)
		got = await backend.query_logs(
			mid, 1, after_dt=datetime.fromtimestamp(t0 + 1, tz=timezone.utc)
		)
		assert [r.message for r in got] == ["b", "c"]

	async def test_query_logs_limit(self, backend: PersistenceBackend) -> None:
		mid = _uniq("m")
		await backend.write_runs([_run(mid=mid, attempt=1)])
		t0 = time.time()
		await backend.write_logs(
			[
				_log(mid=mid, ts=datetime.fromtimestamp(t0 + i, tz=timezone.utc), message=f"m{i}")
				for i in range(10)
			]
		)
		got = await backend.query_logs(mid, 1, limit=3)
		assert len(got) == 3
		assert [r.message for r in got] == ["m0", "m1", "m2"]

	async def test_prune_removes_old_runs_and_their_logs(self, backend: PersistenceBackend) -> None:
		now = _now()
		old_mid = _uniq("old")
		new_mid = _uniq("new")
		await backend.write_runs(
			[
				_run(mid=old_mid, finished_at=now - timedelta(seconds=1000), task="x"),
				_run(mid=new_mid, finished_at=now, task="x"),
			]
		)
		await backend.write_logs(
			[
				_log(mid=old_mid, attempt=1, message="old"),
				_log(mid=new_mid, attempt=1, message="new"),
			]
		)

		deleted = await backend.prune(now - timedelta(seconds=500))
		assert deleted == 1

		remaining = await backend.query_runs(task="x")
		assert [r.message_id for r in remaining] == [new_mid]

		assert await backend.query_logs(old_mid, 1) == []
		surviving = await backend.query_logs(new_mid, 1)
		assert len(surviving) == 1


class TestSqliteSpecific:
	def test_parse_sqlite_dsn_variants(self) -> None:
		assert _parse_sqlite_dsn("sqlite://") == ":memory:"
		assert _parse_sqlite_dsn("sqlite:///tmp/x.db") == "tmp/x.db"
		assert _parse_sqlite_dsn("sqlite://relative.db") == "relative.db"
		assert _parse_sqlite_dsn("plain.db") == "plain.db"

	async def test_in_memory_dsn_works_end_to_end(self) -> None:
		cfg = PersistenceConfig(
			enable=True,
			dsn="sqlite://",
			runs_table=_uniq("runs"),
			logs_table=_uniq("logs"),
		)
		be = SqliteBackend(cfg)
		await be.connect()
		try:
			await be.init_schema()
			mid = _uniq("m")
			await be.write_runs([_run(mid=mid, task="mem")])
			got = await be.query_runs(task="mem")
			assert len(got) == 1
		finally:
			await be.close()

	def test_invalid_table_name_rejected(self) -> None:
		bad = PersistenceConfig(
			enable=True,
			dsn="sqlite://",
			runs_table="bad table",
			logs_table="logs",
		)
		with pytest.raises(ValueError):
			SqliteBackend(bad)


class TestFactory:
	def test_disabled_returns_noop(self) -> None:
		assert isinstance(create_backend(PersistenceConfig(enable=False)), NoopBackend)

	def test_sqlite_dsn_dispatch(self) -> None:
		be = create_backend(PersistenceConfig(enable=True, dsn="sqlite://"))
		assert isinstance(be, SqliteBackend)

	def test_postgres_dsn_dispatch(self) -> None:
		be = create_backend(PersistenceConfig(enable=True, dsn="postgresql://u:p@h:5432/d"))
		assert isinstance(be, PostgresBackend)

	def test_unknown_dsn_raises(self) -> None:
		with pytest.raises(ValueError):
			create_backend(PersistenceConfig(enable=True, dsn="mysql://x"))

	def test_validate_table_name(self) -> None:
		assert validate_table_name("ok_1") == "ok_1"
		with pytest.raises(ValueError):
			validate_table_name("1bad")
		with pytest.raises(ValueError):
			validate_table_name("with space")
		with pytest.raises(ValueError):
			validate_table_name("a" * 100)


async def _wait_for(
	predicate: Callable[[], bool] | Callable[[], Awaitable[bool]], *, timeout: float = 4.0
) -> None:
	deadline = anyio.current_time() + timeout
	while anyio.current_time() < deadline:
		res = predicate()
		if hasattr(res, "__await__"):
			res = await res  # type: ignore[assignment, misc]
		if res:
			return
		await anyio.sleep(0.05)
	raise AssertionError("timed out waiting for condition")


def _evt(
	kind: str,
	*,
	mid: str,
	task: str = "t",
	queue: str = "q",
	worker_pid: int = 7,
	attempt: int = 1,
	ts: float | None = None,
	exc_type: str | None = None,
	exc_message: str | None = None,
) -> Event:
	return Event(
		kind=kind,
		task=task,
		queue=queue,
		worker_pid=worker_pid,
		attempt=attempt,
		message_id=mid,
		ts=ts if ts is not None else time.time(),
		exc_type=exc_type,
		exc_message=exc_message,
	)


async def _run_worker(
	worker: PersistenceWorker,
	body: Callable[[anyio.Event], Awaitable[Any]],
	*,
	timeout: float = 8.0,
) -> None:
	stop = anyio.Event()

	async def _drive():
		try:
			await body(stop)
		finally:
			stop.set()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(worker.run, stop)
			tg.start_soon(_drive)


class TestPersistenceWorker:
	async def test_event_pair_writes_run_with_elapsed(self, sqlite_backend: SqliteBackend) -> None:
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)
		mid = _uniq("m")

		async def _body(stop: anyio.Event) -> None:
			w.enqueue_event("inst-A", _evt("enqueued", mid=mid, task="alpha"))
			await anyio.sleep(0.05)
			w.enqueue_event("inst-A", _evt("succeeded", mid=mid, task="alpha"))

			async def _has_run() -> bool:
				rows = await sqlite_backend.query_runs(task="alpha")
				return len(rows) >= 1

			await _wait_for(_has_run)

		await _run_worker(w, _body)

		rows = await sqlite_backend.query_runs(task="alpha")
		assert len(rows) == 1
		assert rows[0].status == "succeeded"
		assert rows[0].instance_id == "inst-A"
		assert rows[0].worker_pid == 7
		assert rows[0].time_elapsed is not None and rows[0].time_elapsed >= timedelta(seconds=0)
		assert rows[0].finished_at is not None

	async def test_finish_without_start_still_records(self, sqlite_backend: SqliteBackend) -> None:
		"""dropping the start frame must not lose the terminal event"""
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)
		mid = _uniq("m")

		async def _body(stop: anyio.Event) -> None:
			w.enqueue_event(
				"inst-X",
				_evt("failed", mid=mid, task="lonely", exc_type="RuntimeError", exc_message="boom"),
			)

			async def _has_run() -> bool:
				return len(await sqlite_backend.query_runs(task="lonely")) >= 1

			await _wait_for(_has_run)

		await _run_worker(w, _body)

		rows = await sqlite_backend.query_runs(task="lonely")
		assert len(rows) == 1
		assert rows[0].status == "failed"
		assert rows[0].exc_type == "RuntimeError"
		assert rows[0].exc_message == "boom"
		assert rows[0].time_elapsed is None

	async def test_event_with_no_message_id_is_ignored(self, sqlite_backend: SqliteBackend) -> None:
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)

		async def _body(stop: anyio.Event) -> None:
			w.enqueue_event(
				"inst",
				Event(kind="succeeded", task="ghost", queue="q", worker_pid=1, message_id=None),
			)
			await anyio.sleep(0.4)

		await _run_worker(w, _body)

		assert await sqlite_backend.query_runs(task="ghost") == []

	async def test_log_batch_is_persisted(self, sqlite_backend: SqliteBackend) -> None:
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)
		mid = _uniq("m")
		t0 = time.time()
		batch = LogBatch(
			records=[
				LogRecord(
					message_id=mid, attempt=1, level=20, logger="lg", message="one", ts=t0 + 0.001
				),
				LogRecord(
					message_id=mid, attempt=1, level=30, logger="lg", message="two", ts=t0 + 0.002
				),
			]
		)

		async def _body(stop: anyio.Event) -> None:
			w.enqueue_log_batch("inst", batch)

			async def _has_logs() -> bool:
				return len(await sqlite_backend.query_logs(mid, 1)) >= 2

			await _wait_for(_has_logs)

		await _run_worker(w, _body)

		got = await sqlite_backend.query_logs(mid, 1)
		assert [r.message for r in got] == ["one", "two"]
		assert got[1].level == 30

	async def test_queue_overflow_increments_drop_counter(
		self, sqlite_backend: SqliteBackend
	) -> None:
		"""flooding before drain starts -> excess events are dropped, counter reflects it"""
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)
		# shrink queue to make overflow trivial
		import asyncio

		w._queue = asyncio.Queue(maxsize=2)

		# fill capacity + 3 extras
		for i in range(5):
			w.enqueue_event("i", _evt("succeeded", mid=f"mm-{i}"))

		assert w._dropped_events == 3

		# logs use their own counter; flood with more records than capacity left
		for i in range(2):
			w.enqueue_log_batch(
				"i",
				LogBatch(
					records=[
						LogRecord(
							message_id=f"mm-l-{i}",
							attempt=1,
							level=10,
							logger="x",
							message="z",
							ts=time.time(),
						),
					]
				),
			)

		# queue was full from the events above; both log batches should have been dropped.
		assert w._dropped_logs == 2

	async def test_final_flush_on_stop(self, sqlite_backend: SqliteBackend) -> None:
		"""stop_event triggers a force-flush that drains the queue"""
		w = PersistenceWorker(sqlite_backend, sqlite_backend._cfg)
		mid = _uniq("m")

		async def _body(stop: anyio.Event) -> None:
			# enqueue right before stop; with no sleep the drain loop may not have
			# pulled them yet, but the post-stop drain must still flush them.
			w.enqueue_event("inst", _evt("enqueued", mid=mid, task="late"))
			w.enqueue_event("inst", _evt("succeeded", mid=mid, task="late"))
			# stop will be set when _body returns (via _run_worker)

		await _run_worker(w, _body)

		rows = await sqlite_backend.query_runs(task="late")
		assert len(rows) == 1
		assert rows[0].status == "succeeded"


class TestNoopBackend:
	async def test_noop_methods_return_safely(self) -> None:
		be = NoopBackend()
		await be.connect()
		await be.init_schema()
		await be.write_runs([_run(mid="x")])
		await be.write_logs([_log(mid="x")])
		assert await be.query_runs() == []
		assert await be.query_run_attempts("x") == []
		assert await be.query_logs("x", 1) == []
		assert await be.prune(_now()) == 0
		await be.close()
