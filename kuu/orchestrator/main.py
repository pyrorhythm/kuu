from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import threading
import time
import typing
import uuid
from importlib.metadata import version as _pkg_version
from queue import Empty as _QueueEmpty

import anyio
import anyio.to_thread

from kuu.config import Settings
from kuu.observability import (
	PROTOCOL_VERSION,
	BrokerInfo,
	Bye,
	ByeReason,
	Envelope,
	Event,
	EventKind,
	EventsSink,
	Hello,
	State,
	WorkerSnapshot,
	broker_key,
)
from kuu.orchestrator._dashboard import DashboardRunner
from kuu.orchestrator._scheduler import SchedulerRunner
from kuu.orchestrator._watcher import Watcher
from kuu.orchestrator._worker import WorkerPool

if typing.TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer


log = logging.getLogger("kuu.orchestrator")

STATE_INTERVAL = 2.0


class PresetSupervisor:
	"""runs one preset's workers + scheduler + watcher under a single task group

	when ``events_sink`` is provided, emits hello/state/bye envelopes and
	forwards worker task events to the sink; the embedded dashboard is
	disabled in that mode (caller owns the dashboard)

	when ``events_sink`` is ``None``, behaves as the legacy single-process
	orchestrator with an embedded dashboard that drains the worker queue
	directly
	"""

	config: Settings
	preset: str
	instance_id: str

	_wp: WorkerPool
	_dash: DashboardRunner | None
	_watcher: Watcher
	_sched: SchedulerRunner
	_events_sink: EventsSink | None

	_stop_event: anyio.Event
	_metrics_dir: str | None = None
	_metrics_server: WSGIServer | None = None
	_started_at: float
	_bye_reason: ByeReason = "manual"

	def __init__(
		self,
		config: Settings,
		*,
		preset: str = "default",
		events_sink: EventsSink | None = None,
		instance_id: str | None = None,
		manage_metrics: bool = True,
	) -> None:
		self.config = config
		self.preset = preset
		self.instance_id = instance_id or str(uuid.uuid4())
		self._events_sink = events_sink
		self._wp = WorkerPool(config)
		self._dash = DashboardRunner(self, config) if events_sink is None else None
		self._watcher = Watcher(config, self._wp.on_change_callback)
		self._sched = SchedulerRunner(config)
		self._stop_event = anyio.Event()
		self._started_at = 0.0
		self._manage_metrics = manage_metrics

	async def _signal_listener(self) -> None:
		if threading.current_thread() is not threading.main_thread():
			log.debug("not on main thread; skipping signal handler installation")
			return
		with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
			async for signum in signals:
				log.info("received signal %d, initiating shutdown", signum)
				self._bye_reason = "sigint" if signum == signal.SIGINT else "sigterm"
				self._stop_event.set()
				return

	async def start(self) -> None:
		"""run the supervisor until SIGINT/SIGTERM or ``_stop_event`` fires"""

		log.info(
			"starting supervisor preset=%s instance=%s processes=%d watcher=%s dashboard=%s metrics=%s scheduler=%s",
			self.preset,
			self.instance_id,
			self.config.processes,
			self.config.watch.enable,
			self.config.dashboard.enable,
			self.config.metrics.enable,
			self.config.scheduler.enable,
		)
		self._started_at = time.time()
		try:
			await self._start_metrics_server()
			if self._events_sink is not None:
				self._emit_hello()
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				if self._dash is not None:
					tg.start_soon(self._dash.run, self._stop_event)
				tg.start_soon(self._wp.run, self._stop_event)
				tg.start_soon(self._watcher.run, self._stop_event)
				tg.start_soon(self._sched.run, self._stop_event)
				if self._events_sink is not None:
					tg.start_soon(self._forward_worker_events)
					tg.start_soon(self._emit_state_loop)
				await self._stop_event.wait()
				tg.cancel_scope.cancel()
		except Exception:
			self._bye_reason = "crash"
			log.exception("supervisor loop failed")
		finally:
			self._stop_event.set()
			if self._events_sink is not None:
				self._emit_bye()
			self._stop_metrics_server()

	# === emit

	def _emit(self, body) -> None:
		sink = self._events_sink
		if sink is None:
			return
		try:
			sink.emit(
				Envelope(v=PROTOCOL_VERSION, instance=self.instance_id, ts=time.time(), body=body)
			)
		except Exception:
			log.exception("events sink emit failed")

	def _emit_hello(self) -> None:
		self._emit(self._build_hello())

	def _emit_bye(self) -> None:
		self._emit(Bye(reason=self._bye_reason))

	async def _emit_state_loop(self) -> None:
		while not self._stop_event.is_set():
			self._emit(self._build_state())
			with anyio.move_on_after(STATE_INTERVAL):
				await self._stop_event.wait()

	async def _forward_worker_events(self) -> None:
		"""drain worker mp.Queue tuples; convert to Event envelopes and forward"""
		q = self._wp.events_queue
		while not self._stop_event.is_set():
			try:
				while True:
					event, task, ts, pid = q.get_nowait()
					self._emit_event(event, task, pid, ts)
			except _QueueEmpty:
				pass
			await anyio.sleep(0.1)

	def _emit_event(self, kind: str, task: str, worker_pid: int, ts: float) -> None:
		sink = self._events_sink
		if sink is None:
			return
		try:
			sink.emit(
				Envelope(
					v=PROTOCOL_VERSION,
					instance=self.instance_id,
					ts=ts,
					body=Event(
						kind=typing.cast(EventKind, kind),
						task=task,
						queue="",  # not currently propagated by the worker forwarder
						worker_pid=worker_pid,
					),
				)
			)
		except Exception:
			log.exception("events sink emit failed (event)")

	# === snapshot builders

	def _build_hello(self) -> Hello:
		return Hello(
			preset=self.preset,
			host=socket.gethostname(),
			pid=os.getpid(),
			version=_pkg_version("kuu"),
			started_at=self._started_at,
			broker=self._build_broker_info(),
			scheduler_enabled=self.config.scheduler.enable,
			processes=self.config.processes,
		)

	def _build_broker_info(self) -> BrokerInfo:
		try:
			from kuu._import import import_object

			app = import_object(self.config.app)
			broker = app.broker
			return BrokerInfo(type=type(broker).__name__, key=broker_key(broker))
		except Exception:
			log.exception("could not introspect broker for hello")
			return BrokerInfo(type="unknown", key="")

	def _build_state(self) -> State:
		workers = [
			WorkerSnapshot(pid=p.pid or -1, alive=p.is_alive(), current_task=None)
			for p in self._wp._processes
		]
		return State(workers=workers, jobs=[], queues={})

	# === prometheus

	async def _start_metrics_server(self) -> None:
		if not self._manage_metrics:
			return
		metrics_config = self.config.metrics
		if not metrics_config.enable:
			return

		from kuu.prometheus import serve

		self._metrics_dir = await anyio.mkdtemp(prefix="kuu-prom-")
		os.environ["PROMETHEUS_MULTIPROC_DIR"] = self._metrics_dir

		self._metrics_server, _ = serve(
			host=metrics_config.host,
			port=metrics_config.port,
			multiprocess_dir=self._metrics_dir,
		)
		log.info(
			"prometheus aggregator on %s:%d (dir=%s)",
			metrics_config.host,
			metrics_config.port,
			self._metrics_dir,
		)

	def _stop_metrics_server(self) -> None:
		srv = self._metrics_server
		self._metrics_server = None
		if srv is not None:
			try:
				srv.shutdown()
			except Exception:
				log.exception("failed to shut down metrics server")
		if self._metrics_dir:
			shutil.rmtree(self._metrics_dir, ignore_errors=True)
			self._metrics_dir = None


Orchestrator = PresetSupervisor  # legacy alias; will be removed when ControlPlane lands
