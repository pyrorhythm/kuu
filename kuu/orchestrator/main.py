from __future__ import annotations

import logging
import multiprocessing as mp
import signal
import threading
import uuid
from collections import Counter
from datetime import datetime as dtime
from datetime import timezone as tz
from queue import Empty as _QueueEmpty

import anyio

from kuu.config import Settings
from kuu.observability import (
	Bye,
	ByeReason,
	Cmd,
	CmdResponse,
	EventsSink,
)
from kuu.observability._protocol import Body
from kuu.orchestrator._app_loader import AppLoader
from kuu.orchestrator._command_service import CommandService
from kuu.orchestrator._dashboard import DashboardRunner
from kuu.orchestrator._event_forwarder import EventForwarder
from kuu.orchestrator._scheduler import SchedulerRunner
from kuu.orchestrator._serve import MetricsServer
from kuu.orchestrator._snapshots import SnapshotBuilder
from kuu.orchestrator._watcher import Watcher
from kuu.orchestrator._worker import WorkerPool

log = logging.getLogger("kuu.orchestrator")

STATE_INTERVAL = 2.0


class PresetSupervisor:
	config: Settings
	preset: str
	instance_id: str

	_wp: WorkerPool
	_dash: DashboardRunner | None
	_watcher: Watcher
	_sched: SchedulerRunner
	_events_sink: EventsSink | None

	_stop_event: anyio.Event
	_started_at: dtime
	_bye_reason: ByeReason = "manual"

	def __init__(
		self,
		config: Settings,
		*,
		preset: str = "default",
		events_sink: EventsSink | None = None,
		instance_id: str | None = None,
		manage_metrics: bool = True,
		cmd_in: "mp.Queue[Cmd] | None" = None,
		cmd_out: "mp.Queue[CmdResponse] | None" = None,
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
		self._started_at = dtime.now(tz=tz.utc)
		self._manage_metrics = manage_metrics
		self._cmd_in = cmd_in
		self._cmd_out = cmd_out

		self._app_loader = AppLoader(config)
		self._commands = CommandService(self._app_loader)
		self._metrics = MetricsServer()
		self._forwarder: EventForwarder | None = None
		if events_sink is not None:
			self._forwarder = EventForwarder(
				instance_id=self.instance_id,
				events_sink=events_sink,
				events_queue=self._wp.events_queue,
			)

	@property
	def worker_pool(self) -> WorkerPool:
		return self._wp

	def _snapshots(self) -> SnapshotBuilder:
		fwd = self._forwarder
		return SnapshotBuilder(
			config=self.config,
			preset=self.preset,
			instance_id=self.instance_id,
			started_at=self._started_at,
			app_loader=self._app_loader,
			processes=self._wp.processes,
			inflight=fwd.inflight if fwd is not None else Counter(),
			current_task=fwd.current_task if fwd is not None else {},
		)

	async def _signal_listener(self) -> None:
		if threading.current_thread() is not threading.main_thread():
			log.debug("event=supervisor.not_main_thread")
			return
		with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
			async for signum in signals:
				log.info("event=supervisor.shutdown_signal signum=%d", signum)
				self._bye_reason = "sigint" if signum == signal.SIGINT else "sigterm"
				self._stop_event.set()
				return

	async def start(self) -> None:
		"""run the supervisor until SIGINT/SIGTERM or ``_stop_event`` fires"""

		log.info(
			"event=supervisor.starting preset=%s instance=%s processes=%d watcher=%s dashboard=%s metrics=%s scheduler=%s",
			self.preset,
			self.instance_id,
			self.config.processes,
			self.config.watch.enable,
			self.config.dashboard.enable,
			self.config.metrics.enable,
			self.config.scheduler.enable,
		)
		self._started_at = dtime.now(tz=tz.utc)
		try:
			if self._manage_metrics:
				await self._metrics.start(
					self.config.metrics,
					log_event="event=supervisor.prometheus_serving",
				)
			if self._events_sink is not None:
				self._emit(self._snapshots().build_hello())
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				if self._dash is not None:
					tg.start_soon(self._dash.run, self._stop_event)
				tg.start_soon(self._wp.run, self._stop_event)
				tg.start_soon(self._watcher.run, self._stop_event)
				tg.start_soon(self._sched.run, self._stop_event)
				if self._forwarder is not None:
					tg.start_soon(self._forwarder.run, self._stop_event)
					tg.start_soon(self._emit_state_loop)
				if self._cmd_in is not None and self._cmd_out is not None:
					tg.start_soon(self._command_loop)
				await self._stop_event.wait()
				tg.cancel_scope.cancel()
		except Exception as e:
			self._bye_reason = "crash"
			log.exception("event=supervisor.loop_failed error=%s", e)
		finally:
			self._stop_event.set()
			if self._events_sink is not None:
				self._emit(Bye(reason=self._bye_reason))
			if self._manage_metrics:
				self._metrics.stop()

	def _emit(self, body: Body) -> None:
		sink = self._events_sink
		if sink is None:
			return
		try:
			from kuu.observability import PROTOCOL_VERSION, Envelope

			sink.emit(
				Envelope(
					v=PROTOCOL_VERSION,
					instance=self.instance_id,
					ts=dtime.now(tz=tz.utc),
					body=body,
				)
			)
		except Exception as e:
			log.exception("event=supervisor.events_sink_failed error=%s", e)

	async def _emit_state_loop(self) -> None:
		while not self._stop_event.is_set():
			state = await self._snapshots().build_state()
			self._emit(state)
			with anyio.move_on_after(STATE_INTERVAL):
				await self._stop_event.wait()

	async def _command_loop(self) -> None:
		assert self._cmd_in is not None and self._cmd_out is not None
		while not self._stop_event.is_set():
			try:
				cmd = self._cmd_in.get_nowait()
			except _QueueEmpty:
				await anyio.sleep(0.05)
				continue
			response = await self._commands.dispatch(cmd)
			try:
				self._cmd_out.put_nowait(response)
			except Exception as e:
				log.exception("event=supervisor.cmd_out_failed error=%s", e)
