from __future__ import annotations

import logging
import multiprocessing as mp
import os
import shutil
import signal
import socket
import threading
import typing
import uuid
from collections import Counter
from datetime import datetime as dtime
from datetime import timezone as tz
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
	Cmd,
	CmdResponse,
	EnqueueCmd,
	Envelope,
	Event,
	EventsSink,
	Hello,
	JobSnapshot,
	QueueSnapshot,
	RemoveJobCmd,
	State,
	TaskInfo,
	TaskParam,
	TriggerJobCmd,
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
		self._app: typing.Any = None
		self._inflight: Counter[str] = Counter()
		self._current_task: dict[int, str] = {}
		self._cmd_in = cmd_in
		self._cmd_out = cmd_out

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
				self._emit_bye()
			self._stop_metrics_server()

	# === emit

	def _emit(self, body) -> None:
		sink = self._events_sink
		if sink is None:
			return
		try:
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

	def _emit_hello(self) -> None:
		self._emit(self._build_hello())

	def _emit_bye(self) -> None:
		self._emit(Bye(reason=self._bye_reason))

	async def _emit_state_loop(self) -> None:
		while not self._stop_event.is_set():
			state = await self._build_state_async()
			self._emit(state)
			with anyio.move_on_after(STATE_INTERVAL):
				await self._stop_event.wait()

	async def _forward_worker_events(self) -> None:
		"""drain :class:`WorkerEvent` from the worker pool; track in_flight +
		per-pid current_task; emit Event envelopes for finishing kinds.
		also forwards :class:`LogBatch` objects directly to the events sink."""
		import anyio.lowlevel

		from kuu.observability._protocol import LogBatch

		q = self._wp.events_queue
		while not self._stop_event.is_set():
			try:
				did_work = False
				for _ in range(100):
					try:
						item = q.get_nowait()
						did_work = True
					except _QueueEmpty:
						break

					if isinstance(item, LogBatch):
						self._emit(item)
						continue
					we: Event = item
					if we.kind == "started":
						self._inflight[we.queue] += 1
						self._current_task[we.worker_pid] = we.task
						continue
					if self._inflight[we.queue] > 0:
						self._inflight[we.queue] -= 1
					self._current_task.pop(we.worker_pid, None)
					self._emit_event(we)

				if did_work:
					await anyio.lowlevel.checkpoint()
				else:
					await anyio.sleep(0.1)
			except Exception as e:
				if self._stop_event.is_set():
					break
				log.exception("event=supervisor.event_forwarder_error error=%s", e)
				await anyio.sleep(0.5)

	def _emit_event(self, we: Event) -> None:
		sink = self._events_sink
		if sink is None:
			return
		try:
			sink.emit(
				Envelope(
					v=PROTOCOL_VERSION,
					instance=self.instance_id,
					ts=we.ts,
					body=we,
				)
			)
		except Exception as e:
			log.exception("event=supervisor.events_sink_emit_failed error=%s", e)

	# === command rpc

	async def _command_loop(self) -> None:
		"""drain inbound commands, dispatch, push responses to the shared queue"""
		assert self._cmd_in is not None and self._cmd_out is not None
		while not self._stop_event.is_set():
			cmd: Cmd | None = None
			try:
				cmd = self._cmd_in.get_nowait()
			except _QueueEmpty:
				await anyio.sleep(0.05)
				continue
			response = await self._dispatch_command(cmd)
			try:
				self._cmd_out.put_nowait(response)
			except Exception as e:
				log.exception("event=supervisor.cmd_out_failed error=%s", e)

	async def _dispatch_command(self, cmd: Cmd) -> CmdResponse:
		match cmd:
			case EnqueueCmd(request_id=rid, task=task, args=args, kwargs=kwargs):
				return await self._handle_enqueue(rid, task, args, kwargs)
			case TriggerJobCmd(request_id=rid, job_id=job_id):
				return await self._handle_trigger_job(rid, job_id)
			case RemoveJobCmd(request_id=rid, job_id=job_id):
				return self._handle_remove_job(rid, job_id)
			case _:
				return CmdResponse(
					request_id="?", ok=False, error=f"unknown command: {type(cmd).__name__}"
				)

	async def _handle_enqueue(self, rid: str, task: str, args: list, kwargs: dict) -> CmdResponse:
		from kuu.message import Payload

		app = self._ensure_app()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		try:
			await app.broker.connect()
			await app.enqueue_by_name(task, Payload(args=tuple(args), kwargs=kwargs))
			return CmdResponse(request_id=rid, ok=True)
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	async def _handle_trigger_job(self, rid: str, job_id: str) -> CmdResponse:
		app = self._ensure_app()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		job = next((j for j in app.schedule.jobs if j.id == job_id), None)
		if job is None:
			return CmdResponse(request_id=rid, ok=False, error="job not found")
		try:
			await app.broker.connect()
			await app.enqueue_by_name(
				job.task_name,
				job.args,
				queue=job.queue,
				headers=job.headers,
				max_attempts=job.max_attempts,
			)
			return CmdResponse(request_id=rid, ok=True)
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=str(exc))

	def _handle_remove_job(self, rid: str, job_id: str) -> CmdResponse:
		app = self._ensure_app()
		if app is None:
			return CmdResponse(request_id=rid, ok=False, error="app not loaded")
		before = len(app.schedule.jobs)
		app.schedule.jobs = [j for j in app.schedule.jobs if j.id != job_id]
		if len(app.schedule.jobs) == before:
			return CmdResponse(request_id=rid, ok=False, error="job not found")
		return CmdResponse(request_id=rid, ok=True)

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
			concurrency=self.config.concurrency,
			tasks=self._build_tasks(),
		)

	def _build_tasks(self) -> list[TaskInfo]:
		app = self._ensure_app()
		if app is None:
			return []
		try:
			from kuu._sig import sig_params

			out: list[TaskInfo] = []
			for name in sorted(app.registry.names()):
				task = app.registry.get(name)
				if task is None:
					continue
				try:
					raw_params, has_varargs = sig_params(task.original_func)
				except Exception:
					raw_params, has_varargs = [], False
				params = [
					TaskParam(
						name=p["name"],
						annotation=p["annotation"],
						default=p["default"],
						required=p["required"],
					)
					for p in raw_params
				]
				out.append(
					TaskInfo(
						name=task.task_name,
						queue=task.task_queue,
						max_attempts=task.max_attempts,
						timeout=task.timeout,
						params=params,
						has_varargs=has_varargs,
					)
				)
			return out
		except Exception as e:
			log.exception("event=supervisor.build_hello_failed error=%s", e)
			return []

	def _ensure_app(self) -> typing.Any:
		"""lazy-load and cache the app for introspection (broker, scheduler.jobs)"""
		if self._app is not None:
			return self._app
		try:
			from kuu._import import import_object, import_tasks

			self._app = import_object(self.config.app)  # type:ignore
			import_tasks(self.config.task_modules, pattern=(), fs_discover=False)
		except Exception as e:
			log.exception("event=supervisor.import_failed error=%s", e)
		return self._app

	def _build_broker_info(self) -> BrokerInfo:
		app = self._ensure_app()
		if app is None:
			return BrokerInfo(type="unknown", key="")
		try:
			broker = app.broker
			return BrokerInfo(type=type(broker).__name__, key=broker_key(broker))
		except Exception as e:
			log.exception("event=supervisor.broker_introspect_failed error=%s", e)
			return BrokerInfo(type="unknown", key="")

	async def _build_state_async(self) -> State:
		workers = [
			WorkerSnapshot(
				pid=p.pid or -1,
				alive=p.is_alive(),
				current_task=self._current_task.get(p.pid) if p.pid else None,
			)
			for p in self._wp._processes
		]
		jobs = self._build_jobs()
		queues = await self._build_queues()
		return State(workers=workers, jobs=jobs, queues=queues)

	async def _build_queues(self) -> dict[str, QueueSnapshot]:
		"""one snapshot per known queue: tracked in_flight + best-effort broker depth"""
		known_queues = set(self._inflight.keys())
		app = self._ensure_app()
		if app is not None:
			try:
				known_queues |= set(app.registry.queues() or [app.default_queue])
			except Exception:
				log.warning("event=supervisor.queues_registry_failed")

		out: dict[str, QueueSnapshot] = {}
		for q in known_queues:
			depth = await self._probe_depth(q)
			out[q] = QueueSnapshot(in_flight=self._inflight.get(q, 0), depth=depth)
		return out

	async def _probe_depth(self, queue: str) -> int | None:
		app = self._app
		if app is None:
			return None
		broker = app.broker
		probe = getattr(broker, "queue_depth", None)
		if probe is None:
			return None
		try:
			return await probe(queue)
		except Exception:
			log.debug("event=supervisor.probe_depth_failed queue=%s", queue)
			return None

	def _build_jobs(self) -> list[JobSnapshot]:
		"""snapshot of scheduler jobs; empty if scheduler disabled or app unavailable"""
		if not self.config.scheduler.enable:
			return []
		app = self._ensure_app()
		if app is None:
			return []
		try:
			return [
				JobSnapshot(id=j.id, task=j.task_name, next_run=j.next_run.timestamp())
				for j in app.schedule.jobs
			]
		except Exception as e:
			log.exception("event=supervisor.jobs_snapshot_failed error=%s", e)
			return []

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
			"event=supervisor.prometheus_serving host=%s port=%d dir=%s",
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
			except Exception as e:
				log.exception("event=supervisor.metrics_shutdown_failed error=%s", e)
		if self._metrics_dir:
			shutil.rmtree(self._metrics_dir, ignore_errors=True)
			self._metrics_dir = None
