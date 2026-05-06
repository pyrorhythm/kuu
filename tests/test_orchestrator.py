from __future__ import annotations

from pathlib import Path
from typing import Any, Never
from unittest.mock import MagicMock, patch

import anyio
from watchfiles import Change

from kuu.config import Settings
from kuu.orchestrator._watcher import Watcher
from kuu.orchestrator._worker import WorkerPool


def _config(**overrides: Any) -> Settings:
	defaults: dict[str, Any] = dict(
			app="fake.module:app",
			task_modules=["fake.tasks"],
			queues=["default"],
			processes=2,
			concurrency=4,
			prefetch=2,
			shutdown_timeout=1.0,
	)
	defaults.update(overrides)
	return Settings(**defaults)


def _fake_awatch_factory(batches: list[set[tuple[Change, str]]]):
	async def _gen(*_args: Any, **_kwargs: Any):
		for batch in batches:
			yield batch

	return _gen


class TestWorkerPool:
	@patch("kuu.orchestrator._worker.mp.Process")
	def test_start_workers_spawns_n_processes(self, mock_proc_cls: MagicMock) -> None:
		pool = WorkerPool(_config(processes=3))
		pool._stop_event = anyio.Event()

		anyio.run(pool._start_workers)

		assert mock_proc_cls.call_count == 3
		for call in mock_proc_cls.call_args_list:
			assert call.kwargs["daemon"] is False
			# args must be a tuple; the buggy `args=(cfg)` form passed a Settings instead
			args = call.kwargs["args"]
			assert isinstance(args, tuple) and len(args) == 2
			assert args[0] is pool._config

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_start_workers_short_circuits_when_stop_set(self, mock_proc_cls: MagicMock) -> None:
		pool = WorkerPool(_config(processes=4))
		pool._stop_event = anyio.Event()
		pool._stop_event.set()

		anyio.run(pool._start_workers)

		assert mock_proc_cls.call_count == 0
		assert pool._processes == []

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_stop_workers_terminates_and_joins_alive(self, mock_proc_cls: MagicMock) -> None:
		procs = [MagicMock(), MagicMock()]
		# alive on first poll (terminate gets called), dead on second (no kill)
		for p in procs:
			p.is_alive.side_effect = [True, False]
		mock_proc_cls.side_effect = procs

		async def run() -> None:
			pool = WorkerPool(_config(processes=2))
			pool._stop_event = anyio.Event()
			await pool._start_workers()
			await pool._stop_workers()

		anyio.run(run)

		for p in procs:
			p.terminate.assert_called_once()
			p.join.assert_called_once()
			p.kill.assert_not_called()

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_stop_workers_kills_stragglers(self, mock_proc_cls: MagicMock) -> None:
		procs = [MagicMock(), MagicMock()]
		for p in procs:
			p.is_alive.return_value = True
		mock_proc_cls.side_effect = procs

		async def run() -> None:
			pool = WorkerPool(_config(processes=2))
			pool._stop_event = anyio.Event()
			await pool._start_workers()
			await pool._stop_workers()

		anyio.run(run)

		for p in procs:
			p.kill.assert_called_once()
			# join called twice: once with deadline, once after kill
			assert p.join.call_count == 2

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_stop_workers_is_noop_when_empty(self, mock_proc_cls: MagicMock) -> None:
		pool = WorkerPool(_config(processes=2))
		pool._stop_event = anyio.Event()
		anyio.run(pool._stop_workers)
		mock_proc_cls.assert_not_called()

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_run_spawns_then_stops_on_event(self, mock_proc_cls: MagicMock) -> None:
		procs = [MagicMock()]
		procs[0].is_alive.side_effect = [True, False]
		mock_proc_cls.side_effect = procs

		async def run() -> None:
			pool = WorkerPool(_config(processes=1))
			stop = anyio.Event()
			async with anyio.create_task_group() as tg:
				tg.start_soon(pool.run, stop)
				await anyio.sleep(0.05)
				stop.set()

		anyio.run(run)

		# spawned and torn down via run() finally
		assert mock_proc_cls.call_count == 1
		procs[0].terminate.assert_called_once()

	@patch("kuu.orchestrator._worker.mp.Process")
	def test_on_change_callback_restarts_pool(self, mock_proc_cls: MagicMock) -> None:
		gen1 = [MagicMock(), MagicMock()]
		gen2 = [MagicMock(), MagicMock()]
		for p in gen1 + gen2:
			p.is_alive.side_effect = [True, False]
		mock_proc_cls.side_effect = gen1 + gen2

		async def run() -> None:
			pool = WorkerPool(_config(processes=2))
			pool._stop_event = anyio.Event()
			await pool._start_workers()
			await pool.on_change_callback(set())

		anyio.run(run)

		assert mock_proc_cls.call_count == 4
		for p in gen1:
			p.terminate.assert_called_once()

	def test_processes_list_is_per_instance(self) -> None:
		"""regression: `_processes` was a class-level mutable default."""
		a = WorkerPool(_config())
		b = WorkerPool(_config())
		a._processes.append("sentinel")
		assert b._processes == []


class TestWatcher:
	def test_disabled_returns_immediately(self) -> None:
		callback_calls: list[Any] = []

		async def cb(changes: Any) -> None:
			callback_calls.append(changes)

		watcher = Watcher(_config(), cb)

		anyio.run(watcher.run, anyio.Event())

		assert callback_calls == []

	@patch("kuu.orchestrator._watcher.watchfiles.awatch")
	def test_invokes_callback_per_batch(self, mock_awatch: MagicMock) -> None:
		mock_awatch.side_effect = _fake_awatch_factory([
			{(Change.modified, "/x/a.py")},
			{(Change.added, "/x/b.py")},
		])
		seen: list[Any] = []

		async def cb(changes: Any) -> None:
			seen.append(changes)

		from kuu.config import WatchSettings

		cfg = _config(watch=WatchSettings(enable=True, root=Path("/tmp")))
		anyio.run(Watcher(cfg, cb).run, anyio.Event())

		assert len(seen) == 2

	@patch("kuu.orchestrator._watcher.watchfiles.awatch")
	def test_passes_step_and_debounce_in_ms(self, mock_awatch: MagicMock) -> None:
		mock_awatch.side_effect = _fake_awatch_factory([])

		async def cb(_changes: Any) -> None:
			return None

		from kuu.config import WatchSettings

		cfg = _config(
				watch=WatchSettings(
						enable=True, root=Path("/tmp"), reload_delay=0.05, reload_debounce=0.5
				)
		)
		anyio.run(Watcher(cfg, cb).run, anyio.Event())

		kwargs = mock_awatch.call_args.kwargs
		assert kwargs["step"] == 50
		assert kwargs["debounce"] == 500
		assert kwargs["recursive"] is True
		assert "watch_filter" in kwargs

	@patch("kuu.orchestrator._watcher.watchfiles.awatch")
	def test_filter_excludes_configured_globs(self, mock_awatch: MagicMock) -> None:
		captured: dict[str, Any] = {}

		async def _gen(*_args: Any, **kwargs: Any):
			captured["filter"] = kwargs.get("watch_filter")
			if Never:
				yield set()

		mock_awatch.side_effect = _gen

		async def cb(_changes: Any) -> None:
			return None

		from kuu.config import WatchSettings

		root = Path("/tmp/proj").resolve()
		cfg = _config(
				watch=WatchSettings(
						enable=True,
						root=root,
						respect_gitignore=False,
						exclude=[Path("*.log"), Path("build") / "**"],
				)
		)
		anyio.run(Watcher(cfg, cb).run, anyio.Event())

		f = captured["filter"]
		assert f is not None
		assert f(Change.modified, str(root / "kuu.py")) is True
		assert f(Change.modified, str(root / "x.log")) is False
		assert f(Change.modified, str(root / "deep/dir/y.log")) is False
		assert f(Change.modified, str(root / "build/out.bin")) is False
