from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import anyio
import pytest

from kuu.orchestrator.main import Orchestrator


@pytest.fixture
def base_orchestrator() -> Orchestrator:
	return Orchestrator(
		app_spec="fake.module:app",
		task_modules=["fake.tasks"],
		queues=["default"],
		concurrency=4,
		prefetch=2,
		shutdown_timeout=1.0,
		subprocesses=2,
		path_to_watch=Path("/tmp"),
		reload_delay=0.05,
		reload_debounce=0.05,
	)


def _fake_awatch_factory(batches: list[set[tuple[int, str]]]):
	async def _gen(*_args, **_kwargs):
		for batch in batches:
			yield batch

	return _gen


class TestOrchestratorLifecycle:
	def test_init_prefetch_default(self) -> None:
		o = Orchestrator(app_spec="a:b", task_modules=["t"], concurrency=8)
		assert o.prefetch == 2

	def test_init_custom_prefetch(self) -> None:
		o = Orchestrator(app_spec="a:b", task_modules=["t"], concurrency=8, prefetch=5)
		assert o.prefetch == 5

	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_start_workers_spawns_n_processes(
		self, mock_process_cls: MagicMock, base_orchestrator: Orchestrator
	) -> None:
		anyio.run(base_orchestrator._start_workers)
		assert mock_process_cls.call_count == 2
		for call in mock_process_cls.call_args_list:
			assert call.kwargs["daemon"] is False
			assert call.kwargs["target"].__name__ == "run_worker"

	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_stop_workers_terminates_and_joins(
		self, mock_process_cls: MagicMock, base_orchestrator: Orchestrator
	) -> None:
		procs = [MagicMock(), MagicMock()]
		for p in procs:
			p.is_alive.side_effect = [True, False]
		mock_process_cls.side_effect = procs

		async def run() -> None:
			await base_orchestrator._start_workers()
			await base_orchestrator._stop_workers()

		anyio.run(run)
		for p in procs:
			p.terminate.assert_called_once()
			p.join.assert_called_once()
			p.kill.assert_not_called()

	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_stop_workers_kills_stragglers(
		self, mock_process_cls: MagicMock, base_orchestrator: Orchestrator
	) -> None:
		procs = [MagicMock(), MagicMock()]
		for p in procs:
			p.is_alive.return_value = True
		mock_process_cls.side_effect = procs

		async def run() -> None:
			await base_orchestrator._start_workers()
			await base_orchestrator._stop_workers()

		anyio.run(run)
		for p in procs:
			p.kill.assert_called_once()
			assert p.join.call_count == 2

	def test_start_workers_short_circuits_when_stop_event_set(
		self, base_orchestrator: Orchestrator
	) -> None:
		async def run() -> None:
			base_orchestrator._stop_event.set()
			await base_orchestrator._start_workers()

		anyio.run(run)
		assert base_orchestrator._processes == []

	def test_signal_listener_skips_on_non_main_thread(
		self, base_orchestrator: Orchestrator
	) -> None:
		done = threading.Event()

		def runner() -> None:
			anyio.run(base_orchestrator._signal_listener)
			done.set()

		t = threading.Thread(target=runner)
		t.start()
		t.join(timeout=1.0)
		assert done.is_set(), "signal listener should return immediately off main thread"
		assert not base_orchestrator._stop_event.is_set()


class TestOrchestratorWatchFiles:
	@patch("kuu.orchestrator.main.watchfiles.awatch")
	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_watch_reloads_on_change(
		self,
		mock_process_cls: MagicMock,
		mock_awatch: MagicMock,
		base_orchestrator: Orchestrator,
	) -> None:
		mock_awatch.side_effect = _fake_awatch_factory([{(1, "/tmp/foo.py")}])
		anyio.run(base_orchestrator._watch_files)
		# one change batch → one reload → subprocesses (2) processes spawned
		assert mock_process_cls.call_count == 2

	@patch.object(Orchestrator, "_signal_listener", new=lambda self: _noop_async())
	@patch("kuu.orchestrator.main.watchfiles.awatch")
	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_start_stops_when_stop_event_set(
		self,
		mock_process_cls: MagicMock,
		mock_awatch: MagicMock,
		base_orchestrator: Orchestrator,
	) -> None:
		mock_awatch.side_effect = _fake_awatch_factory([])

		async def run() -> None:
			base_orchestrator._stop_event.set()
			await base_orchestrator.start()

		anyio.run(run)
		mock_awatch.assert_called_once()
		assert mock_process_cls.call_count == 0

	@patch.object(Orchestrator, "_signal_listener", new=lambda self: _noop_async())
	@patch("kuu.orchestrator.main.watchfiles.awatch")
	@patch("kuu.orchestrator.main.multiprocessing.Process")
	def test_watch_converts_delays_to_ms(
		self,
		mock_process_cls: MagicMock,
		mock_awatch: MagicMock,
		base_orchestrator: Orchestrator,
	) -> None:
		mock_awatch.side_effect = _fake_awatch_factory([])

		async def run() -> None:
			base_orchestrator._stop_event.set()
			await base_orchestrator.start()

		anyio.run(run)
		call_kwargs = mock_awatch.call_args.kwargs
		assert call_kwargs["step"] == 50
		assert call_kwargs["debounce"] == 50
		assert call_kwargs["recursive"] is True


async def _noop_async() -> None:
	return None
