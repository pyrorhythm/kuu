from __future__ import annotations

import logging
import multiprocessing as mp
import os
import shutil
import signal
import threading
import typing
import uuid

import anyio
import anyio.to_thread

from kuu.config import Kuunfig, Settings
from kuu.observability import (
	Bye,
	Cmd,
	CmdResponse,
	Envelope,
	Event,
	Hello,
	InMemoryRegistry,
	MpQueueSink,
	MpQueueSource,
	State,
)
from kuu.observability._protocol import LogBatch
from kuu.persistence import PersistenceWorker, create_backend

if typing.TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer

	from kuu.web.dashboard import Dashboard

log = logging.getLogger("kuu.control_plane")


def _request_id_of(cmd: Cmd) -> str:
	rid = getattr(cmd, "request_id", None)
	if not rid:
		raise ValueError("command missing request_id")
	return rid


def _run_supervisor_child(
	config: Settings,
	preset: str,
	queue: mp.Queue[Envelope],
	instance_id: str,
	cmd_in: mp.Queue[Cmd],
	cmd_out: mp.Queue[CmdResponse],
) -> None:
	import anyio as _anyio

	from kuu.orchestrator.main import PresetSupervisor

	sink = MpQueueSink(queue)
	sup = PresetSupervisor(
		config,
		preset=preset,
		events_sink=sink,
		instance_id=instance_id,
		manage_metrics=False,
		cmd_in=cmd_in,
		cmd_out=cmd_out,
	)
	_anyio.run(sup.start)


class ControlPlane:
	kuunfig: Kuunfig

	_events_queue: mp.Queue[Envelope]
	_source: MpQueueSource
	_registry: InMemoryRegistry
	_procs: list[tuple[str, mp.Process]]
	_dashboard: Dashboard | None

	_cmd_responses: mp.Queue[CmdResponse]
	_cmd_in_per_instance: dict[str, mp.Queue[Cmd]]
	_cmd_pending: dict[str, anyio.Event]
	_cmd_results: dict[str, CmdResponse]

	_stop_event: anyio.Event
	_metrics_dir: str | None = None
	_metrics_server: WSGIServer | None = None
	_persist_worker: PersistenceWorker | None = None

	def __init__(self, kuunfig: Kuunfig) -> None:
		self.kuunfig = kuunfig
		self._events_queue = mp.Queue()
		self._source = MpQueueSource(self._events_queue)
		self._registry = InMemoryRegistry()
		self._procs = []
		self._dashboard = None
		self._cmd_responses = mp.Queue()
		self._cmd_in_per_instance = {}
		self._cmd_pending = {}
		self._cmd_results = {}
		self._stop_event = anyio.Event()
		self._persist_worker = self._create_persist_worker()

	async def start(self) -> None:
		instances = self._instances()
		log.info(
			"starting control plane presets=%s",
			[name for name, _ in instances],
		)
		try:
			await self._start_metrics_server(instances)
			self._build_dashboard()
			self._spawn_all(instances)
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				tg.start_soon(self._ingest_loop)
				tg.start_soon(self._cmd_response_loop)
				tg.start_soon(self._roster_log_loop)
				if self._persist_worker is not None:
					tg.start_soon(self._persist_worker.run, self._stop_event)
				if self._dashboard is not None:
					tg.start_soon(self._serve_dashboard)
				await self._stop_event.wait()
				tg.cancel_scope.cancel()
		except Exception:
			log.exception("control plane loop failed")
		finally:
			self._stop_event.set()
			self._source.close()
			await self._stop_children()
			self._stop_metrics_server()

	def _instances(self) -> list[tuple[str, Settings]]:
		if self.kuunfig.presets:
			return [(name, self.kuunfig.resolve(name)) for name in self.kuunfig.presets]
		return [("default", self.kuunfig.default)]

	def _spawn_all(self, instances: list[tuple[str, Settings]]) -> None:
		for preset, cfg in instances:
			instance_id = str(uuid.uuid4())
			cmd_in: mp.Queue[Cmd] = mp.Queue()
			self._cmd_in_per_instance[instance_id] = cmd_in
			p = mp.Process(
				target=_run_supervisor_child,
				args=(cfg, preset, self._events_queue, instance_id, cmd_in, self._cmd_responses),
				name=f"kuu-supervisor-{preset}",
				daemon=False,
			)
			p.start()
			self._procs.append((preset, p))
			log.info("spawned supervisor preset=%s pid=%s instance=%s", preset, p.pid, instance_id)

	async def _signal_listener(self) -> None:
		if threading.current_thread() is not threading.main_thread():
			return
		with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
			async for signum in signals:
				log.info("received signal %d, shutting down", signum)
				self._stop_event.set()
				return

	async def _ingest_loop(self) -> None:
		async for env in self._source:
			self._registry.ingest(env)
			match env.body:
				case Hello() as h:
					log.info(
						"hello instance=%s preset=%s broker=%s/%s pid=%d",
						env.instance,
						h.preset,
						h.broker.type,
						h.broker.key[:12],
						h.pid,
					)
				case Bye() as b:
					log.info("bye   instance=%s reason=%s", env.instance, b.reason)
				case Event() as e:
					if self._dashboard is not None:
						self._dashboard.stats.ingest(e.kind, e.task, env.ts)
					if self._persist_worker is not None:
						self._persist_worker.enqueue_event(env.instance, e)
				case State():
					pass
				case LogBatch() as lb:
					if self._persist_worker is not None:
						self._persist_worker.enqueue_log_batch(env.instance, lb)
				case _:
					pass

	async def _roster_log_loop(self) -> None:
		while not self._stop_event.is_set():
			roster = self._registry.all()
			if roster:
				log.debug(
					"roster: %s",
					", ".join(
						f"{e.hello.preset}/{e.instance_id[:8]}"
						f"({len(e.last_state.workers) if e.last_state else 0}w)"
						for e in roster
					),
				)
			with anyio.move_on_after(2.0):
				await self._stop_event.wait()

	async def _stop_children(self) -> None:
		if not self._procs:
			return
		log.info("stopping %d supervisor process(es)", len(self._procs))

		def _terminate_and_wait() -> None:
			import time as _time

			for _, p in self._procs:
				if p.is_alive():
					p.terminate()
			deadline = _time.monotonic() + 30.0
			for _, p in self._procs:
				remaining = deadline - _time.monotonic()
				if remaining > 0:
					p.join(timeout=remaining)
			for _, p in self._procs:
				if p.is_alive():
					log.warning("supervisor %s did not terminate, killing", p.pid)
					p.kill()
					p.join(timeout=5)

		await anyio.to_thread.run_sync(_terminate_and_wait)
		self._procs = []

	async def send_command(self, instance_id: str, cmd: Cmd, timeout: float = 10.0) -> CmdResponse:
		cmd_in = self._cmd_in_per_instance.get(instance_id)
		if cmd_in is None:
			raise KeyError(f"unknown instance {instance_id!r}")
		rid = _request_id_of(cmd)
		event = anyio.Event()
		self._cmd_pending[rid] = event
		try:
			cmd_in.put_nowait(cmd)
		except Exception as exc:
			self._cmd_pending.pop(rid, None)
			return CmdResponse(request_id=rid, ok=False, error=f"send failed: {exc}")
		try:
			with anyio.fail_after(timeout):
				await event.wait()
		except TimeoutError:
			self._cmd_pending.pop(rid, None)
			return CmdResponse(request_id=rid, ok=False, error="timeout")
		return self._cmd_results.pop(rid)

	async def _cmd_response_loop(self) -> None:
		from queue import Empty as _QueueEmpty

		while not self._stop_event.is_set():
			try:
				while True:
					resp = self._cmd_responses.get_nowait()
					ev = self._cmd_pending.pop(resp.request_id, None)
					if ev is not None:
						self._cmd_results[resp.request_id] = resp
						ev.set()
			except _QueueEmpty:
				pass
			await anyio.sleep(0.05)

	def _create_persist_worker(self) -> PersistenceWorker | None:
		cfg = self.kuunfig.default.persistence
		if not cfg.enable:
			log.info("persistence: disabled")
			return None
		backend = create_backend(cfg)
		return PersistenceWorker(backend, cfg)

	def _backend_for_dashboard(self):
		pw = self._persist_worker
		if pw is None:
			return None
		return pw._backend  # type: ignore[attr-defined]

	def _build_dashboard(self) -> None:
		dash_cfg = self.kuunfig.default.dashboard
		if not dash_cfg.enable:
			return
		try:
			from kuu.web.dashboard import Dashboard
		except ImportError:
			log.exception("dashboard dependencies missing; install kuu[dashboard]")
			return

		import os as _os

		token = _os.environ.get("KUU_DASHBOARD_TOKEN")
		self._dashboard = Dashboard(
			registry=self._registry,
			control=self,
			ingest_token=token,
			persistence_backend=self._backend_for_dashboard(),
		)

	async def _serve_dashboard(self) -> None:
		dash = self._dashboard
		if dash is None:
			return
		import uvicorn

		dash_cfg = self.kuunfig.default.dashboard
		asgi_app = dash.build_app()
		if dash_cfg.path and dash_cfg.path != "/":
			from starlette.applications import Starlette
			from starlette.routing import Mount

			asgi_app = Starlette(routes=[Mount(dash_cfg.path, app=asgi_app)])

		cfg = uvicorn.Config(
			asgi_app,
			host=dash_cfg.host,
			port=dash_cfg.port,
			log_level="warning",
		)
		server = uvicorn.Server(cfg)
		log.info(
			"dashboard serving on http://%s:%d%s",
			dash_cfg.host,
			dash_cfg.port,
			dash_cfg.path,
		)
		try:
			async with anyio.create_task_group() as tg:
				tg.start_soon(server.serve)
				await self._stop_event.wait()
				tg.cancel_scope.cancel()
		finally:
			with anyio.move_on_after(delay=5.0):
				await server.shutdown()
			if server.started:
				server.force_exit = True

	async def _start_metrics_server(self, instances: list[tuple[str, Settings]]) -> None:
		metrics_cfg = next(
			(cfg.metrics for _, cfg in instances if cfg.metrics.enable),
			None,
		)
		if metrics_cfg is None:
			return

		from kuu.prometheus import serve

		self._metrics_dir = await anyio.mkdtemp(prefix="kuu-prom-")
		os.environ["PROMETHEUS_MULTIPROC_DIR"] = self._metrics_dir
		self._metrics_server, _ = serve(
			host=metrics_cfg.host,
			port=metrics_cfg.port,
			multiprocess_dir=self._metrics_dir,
		)
		log.info(
			"prometheus aggregator on %s:%d (dir=%s)",
			metrics_cfg.host,
			metrics_cfg.port,
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
