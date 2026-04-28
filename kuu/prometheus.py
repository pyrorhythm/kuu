from __future__ import annotations

import os
import time
from threading import Thread
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID
from wsgiref.simple_server import WSGIServer

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram
from typing_extensions import Doc

if TYPE_CHECKING:
	from .app import Kuu
	from .context import Context
	from .message import Message
	from .middleware.base import Next


_DEFAULT_DURATION_BUCKETS = (
	0.005,
	0.01,
	0.025,
	0.05,
	0.1,
	0.25,
	0.5,
	1.0,
	2.5,
	5.0,
	10.0,
	30.0,
	60.0,
)
_DEFAULT_RETRY_DELAY_BUCKETS = (0.0, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0)
_DEFAULT_ENQUEUE_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0)


class WorkerMetrics:
	"""
	Worker-side, event-driven Prometheus metrics. Wires onto `app.events`.

	Instantiate inside each worker subprocess after the app/registry is loaded.
	Exposes per-task counters/histograms and an in-flight gauge. In multiprocess
	mode (PROMETHEUS_MULTIPROC_DIR set) the gauge sums across living workers.

	Metric names ({ns} = namespace, default "qq"):
	  - {ns}_tasks_received_total{task,queue}
	  - {ns}_tasks_started_total{task,queue}
	  - {ns}_tasks_succeeded_total{task,queue}
	  - {ns}_tasks_retried_total{task,queue}
	  - {ns}_tasks_failed_total{task,queue,exc}
	  - {ns}_tasks_dead_total{task,queue}
	  - {ns}_task_duration_seconds{task,queue}      (from task_succeeded)
	  - {ns}_task_retry_delay_seconds{task,queue}   (from task_retried)
	  - {ns}_tasks_in_flight{task,queue}            gauge (livesum across procs)

	`in_flight` is decremented on the first terminal event per message id
	(succeeded/retried/failed/dead) to handle the Fail->Dead chain and the
	standalone Reject->Dead path without double-counting.
	"""

	def __init__(
		self,
		app: Kuu,
		namespace: str = "kuu",
		registry: CollectorRegistry = REGISTRY,
		duration_buckets: tuple[float, ...] = _DEFAULT_DURATION_BUCKETS,
		retry_delay_buckets: tuple[float, ...] = _DEFAULT_RETRY_DELAY_BUCKETS,
	):
		self._inflight_ids: set[UUID] = set()

		self.received = Counter(
			f"{namespace}_tasks_received_total",
			"Tasks received by worker",
			("task", "queue"),
			registry=registry,
		)
		self.started = Counter(
			f"{namespace}_tasks_started_total",
			"Tasks started processing",
			("task", "queue"),
			registry=registry,
		)
		self.succeeded = Counter(
			f"{namespace}_tasks_succeeded_total",
			"Tasks succeeded",
			("task", "queue"),
			registry=registry,
		)
		self.retried = Counter(
			f"{namespace}_tasks_retried_total",
			"Tasks scheduled for retry",
			("task", "queue"),
			registry=registry,
		)
		self.failed = Counter(
			f"{namespace}_tasks_failed_total",
			"Tasks raised an exception",
			("task", "queue", "exc"),
			registry=registry,
		)
		self.dead = Counter(
			f"{namespace}_tasks_dead_total",
			"Tasks reached terminal state without success",
			("task", "queue"),
			registry=registry,
		)
		self.duration = Histogram(
			f"{namespace}_task_duration_seconds",
			"Task processing duration",
			("task", "queue"),
			buckets=duration_buckets,
			registry=registry,
		)
		self.retry_delay = Histogram(
			f"{namespace}_task_retry_delay_seconds",
			"Scheduled retry delay",
			("task", "queue"),
			buckets=retry_delay_buckets,
			registry=registry,
		)
		self.in_flight = Gauge(
			f"{namespace}_tasks_in_flight",
			"Tasks currently being processed",
			("task", "queue"),
			multiprocess_mode="livesum",
			registry=registry,
		)

		ev = app.events
		ev.task_received.connect(self._on_received)
		ev.task_started.connect(self._on_started)
		ev.task_succeeded.connect(self._on_succeeded)
		ev.task_retried.connect(self._on_retried)
		ev.task_failed.connect(self._on_failed)
		ev.task_dead.connect(self._on_dead)

	def _settle(self, msg: Message) -> None:
		if msg.id in self._inflight_ids:
			self._inflight_ids.discard(msg.id)
			self.in_flight.labels(msg.task, msg.queue).dec()

	def _on_received(self, msg: Message) -> None:
		self.received.labels(msg.task, msg.queue).inc()

	def _on_started(self, msg: Message) -> None:
		self.started.labels(msg.task, msg.queue).inc()
		self._inflight_ids.add(msg.id)
		self.in_flight.labels(msg.task, msg.queue).inc()

	def _on_succeeded(self, msg: Message, elapsed: float) -> None:
		self.duration.labels(msg.task, msg.queue).observe(elapsed)
		self.succeeded.labels(msg.task, msg.queue).inc()
		self._settle(msg)

	def _on_retried(self, msg: Message, delay: float) -> None:
		self.retry_delay.labels(msg.task, msg.queue).observe(delay)
		self.retried.labels(msg.task, msg.queue).inc()
		self._settle(msg)

	def _on_failed(self, msg: Message, exc: BaseException) -> None:
		self.failed.labels(msg.task, msg.queue, type(exc).__name__).inc()
		self._settle(msg)

	def _on_dead(self, msg: Message) -> None:
		self.dead.labels(msg.task, msg.queue).inc()
		self._settle(msg)


class ClientMetrics:
	"""
	Client-side metrics, implemented as a Middleware. Add to `Kuu(middleware=[...])`.

	Acts only on the `enqueue` phase. Records:
	  - {ns}_client_enqueued_total{task,queue}
	  - {ns}_client_enqueue_errors_total{task,queue,exc}
	  - {ns}_client_enqueue_duration_seconds{task,queue}  (broker handoff latency)
	"""

	def __init__(
		self,
		namespace: str = "kuu",
		registry: CollectorRegistry = REGISTRY,
		duration_buckets: tuple[float, ...] = _DEFAULT_ENQUEUE_BUCKETS,
	):
		self.enqueued = Counter(
			f"{namespace}_client_enqueued_total",
			"Tasks enqueued by client",
			("task", "queue"),
			registry=registry,
		)
		self.errors = Counter(
			f"{namespace}_client_enqueue_errors_total",
			"Enqueue attempts that raised",
			("task", "queue", "exc"),
			registry=registry,
		)
		self.duration = Histogram(
			f"{namespace}_client_enqueue_duration_seconds",
			"Time spent enqueueing to broker",
			("task", "queue"),
			buckets=duration_buckets,
			registry=registry,
		)

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase != "enqueue":
			return await call_next()

		msg = ctx.message
		t0 = time.perf_counter()
		try:
			result = await call_next()
		except BaseException as exc:
			self.errors.labels(msg.task, msg.queue, type(exc).__name__).inc()
			raise
		else:
			self.enqueued.labels(msg.task, msg.queue).inc()
			return result
		finally:
			self.duration.labels(msg.task, msg.queue).observe(time.perf_counter() - t0)


def serve(
	host: Annotated[str, Doc("Host to bind the /metrics HTTP server to.")] = "0.0.0.0",
	port: Annotated[int, Doc("Port to bind the /metrics HTTP server to.")] = 9100,
	multiprocess_dir: Annotated[
		str | None,
		Doc("Directory for prometheus_client multiprocess shared state. When set, aggregates metrics across all worker subprocesses that wrote to this directory."),
	] = None,
) -> tuple[WSGIServer, Thread]:
	"""
	Start a `/metrics` HTTP endpoint. Returns (server, thread) from prometheus_client.

	If `multiprocess_dir` is given (or `PROMETHEUS_MULTIPROC_DIR` env is set),
	aggregates metrics from on-disk shared state across all worker subprocesses
	that wrote to that dir. Otherwise serves the in-process REGISTRY directly.

	Call this in the orchestrator (parent) process when running multiple workers,
	or in-process for a single-worker setup.
	"""
	from prometheus_client import start_http_server

	mp_dir = multiprocess_dir or os.environ.get("PROMETHEUS_MULTIPROC_DIR")
	if mp_dir:
		from prometheus_client.multiprocess import MultiProcessCollector

		os.environ["PROMETHEUS_MULTIPROC_DIR"] = mp_dir
		registry = CollectorRegistry()
		MultiProcessCollector(registry, path=mp_dir)
		return start_http_server(port, addr=host, registry=registry)
	return start_http_server(port, addr=host)


def asgi_app(multiprocess_dir: str | None = None) -> Any:
	"""
	Build an ASGI app exposing `/metrics`. Mount it into your own ASGI
	application (FastAPI, Starlette, etc.).

	If `multiprocess_dir` is given (or `PROMETHEUS_MULTIPROC_DIR` env is set),
	aggregates metrics from on-disk shared state across worker subprocesses.
	Otherwise serves the in-process REGISTRY directly.

	Examples:
	    # Client side, FastAPI:
	    from fastapi import FastAPI
	    from kuu.prometheus import ClientMetrics, asgi_app

	    app = FastAPI()
	    kuu_app = Kuu(broker=..., middleware=[ClientMetrics()])
	    app.mount("/metrics", asgi_app())

	    # Worker side, exposing aggregated multiproc metrics from a sidecar
	    # ASGI server (e.g. uvicorn) instead of the orchestrator's WSGI server:
	    app = Starlette(routes=[Mount("/metrics", app=asgi_app(multiprocess_dir="/tmp/kuu"))])
	"""
	from prometheus_client import make_asgi_app

	mp_dir = multiprocess_dir or os.environ.get("PROMETHEUS_MULTIPROC_DIR")
	if mp_dir:
		from prometheus_client.multiprocess import MultiProcessCollector

		os.environ["PROMETHEUS_MULTIPROC_DIR"] = mp_dir
		registry = CollectorRegistry()
		MultiProcessCollector(registry, path=mp_dir)
		return make_asgi_app(registry=registry)
	return make_asgi_app()


def mark_worker_dead(pid: int) -> None:
	"""
	Tell prometheus_client a worker subprocess is gone, so its files stop
	contributing to `livesum` gauges. No-op outside multiprocess mode.
	"""
	if not os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
		return
	from prometheus_client.multiprocess import mark_process_dead

	mark_process_dead(pid)
