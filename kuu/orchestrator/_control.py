"""control plane: forks one PresetSupervisor per preset and aggregates events

each child runs an independent supervisor in its own subprocess; all
children share a single ``mp.Queue`` that the parent drains as an
``EventsSource``, feeding an ``InstanceRegistry``

prometheus aggregation is owned by the control plane (one server, one
multiprocess dir inherited by every child via env)

dashboard is not yet wired here; that lands in step 3b
"""

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
	Envelope,
	Hello,
	InMemoryRegistry,
	MpQueueSink,
	MpQueueSource,
	State,
)

if typing.TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer


log = logging.getLogger("kuu.control_plane")


def _run_supervisor_child(
	config: Settings,
	preset: str,
	queue: "mp.Queue[Envelope]",
	instance_id: str,
) -> None:
	"""child entrypoint: build a sink-driven supervisor and run it"""
	import anyio as _anyio

	from kuu.orchestrator.main import PresetSupervisor

	sink = MpQueueSink(queue)
	sup = PresetSupervisor(
		config,
		preset=preset,
		events_sink=sink,
		instance_id=instance_id,
		manage_metrics=False,
	)
	_anyio.run(sup.start)


class ControlPlane:
	"""orchestrates N preset supervisors as subprocesses"""

	kuunfig: Kuunfig

	_events_queue: "mp.Queue[Envelope]"
	_source: MpQueueSource
	_registry: InMemoryRegistry
	_procs: list[tuple[str, mp.Process]]

	_stop_event: anyio.Event
	_metrics_dir: str | None = None
	_metrics_server: WSGIServer | None = None

	def __init__(self, kuunfig: Kuunfig) -> None:
		self.kuunfig = kuunfig
		self._events_queue = mp.Queue()
		self._source = MpQueueSource(self._events_queue)
		self._registry = InMemoryRegistry()
		self._procs = []
		self._stop_event = anyio.Event()

	async def start(self) -> None:
		instances = self._instances()
		log.info(
			"starting control plane presets=%s",
			[name for name, _ in instances],
		)
		try:
			await self._start_metrics_server(instances)
			self._spawn_all(instances)
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._signal_listener)
				tg.start_soon(self._ingest_loop)
				tg.start_soon(self._roster_log_loop)
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
		"""decide which supervisors to spawn

		if ``presets`` is non-empty, spawn one per preset using the
		resolved settings; otherwise spawn a single ``default`` supervisor
		"""
		if self.kuunfig.presets:
			return [(name, self.kuunfig.resolve(name)) for name in self.kuunfig.presets]
		return [("default", self.kuunfig.default)]

	def _spawn_all(self, instances: list[tuple[str, Settings]]) -> None:
		for preset, cfg in instances:
			instance_id = str(uuid.uuid4())
			p = mp.Process(
				target=_run_supervisor_child,
				args=(cfg, preset, self._events_queue, instance_id),
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
						env.instance, h.preset, h.broker.type, h.broker.key[:12], h.pid,
					)
				case Bye() as b:
					log.info("bye   instance=%s reason=%s", env.instance, b.reason)
				case State():
					pass
				case _:
					pass

	async def _roster_log_loop(self) -> None:
		"""diagnostic: print live roster every state interval"""
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

	# === prometheus

	async def _start_metrics_server(self, instances: list[tuple[str, Settings]]) -> None:
		"""start one prometheus aggregator if any preset enables metrics

		uses the first metrics-enabled preset's host/port; child processes
		inherit ``PROMETHEUS_MULTIPROC_DIR`` from the parent env
		"""
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
			metrics_cfg.host, metrics_cfg.port, self._metrics_dir,
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
