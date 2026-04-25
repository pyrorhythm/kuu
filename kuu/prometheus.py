from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

if TYPE_CHECKING:
	from .app import Kuu
	from .message import Message


class PrometheusMetrics:
	"""
	Event/signal-based Prometheus integration.

	Connects to `app.events` signals; exposes per-task metrics:
	  - {ns}_tasks_enqueued_total{task,queue}
	  - {ns}_tasks_received_total{task,queue}
	  - {ns}_tasks_started_total{task,queue}
	  - {ns}_tasks_succeeded_total{task,queue}
	  - {ns}_tasks_retried_total{task,queue}
	  - {ns}_tasks_failed_total{task,queue,exc}
	  - {ns}_tasks_dead_total{task,queue}
	  - {ns}_task_duration_seconds{task,queue}     histogram (from task_succeeded)
	  - {ns}_task_retry_delay_seconds{task,queue}  histogram (from task_retried)
	  - {ns}_tasks_in_flight{task,queue}           gauge

	`in_flight` is decremented on the first terminal event per message id
	(succeeded/retried/failed/dead) to handle both the Fail→Dead chain and
	the standalone Reject→Dead path without double-counting.
	"""

	_default_duration_buckets = (
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

	_default_retry_delay_buckets = (
		0.0,
		0.1,
		0.5,
		1.0,
		5.0,
		15.0,
		60.0,
		300.0,
		900.0,
	)

	def __init__(
		self,
		app: Kuu,
		namespace: str = "qq",
		registry: CollectorRegistry = REGISTRY,
		duration_buckets: tuple[float, ...] = _default_duration_buckets,
		retry_delay_buckets: tuple[float, ...] = _default_retry_delay_buckets,
	):
		self._inflight_ids: set[UUID] = set()

		self.enqueued = Counter(
			f"{namespace}_tasks_enqueued_total",
			"Tasks enqueued",
			("task", "queue"),
			registry=registry,
		)
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
			registry=registry,
		)

		ev = app.events
		ev.task_enqueued.connect(self._on_enqueued)
		ev.task_received.connect(self._on_received)
		ev.task_started.connect(self._on_started)
		ev.task_succeeded.connect(self._on_succeeded)
		ev.task_retried.connect(self._on_retried)
		ev.task_failed.connect(self._on_failed)
		ev.task_dead.connect(self._on_dead)

	def _settle(self, msg: Message) -> bool:
		"""Decrement in_flight at most once per message id. Returns True if decremented."""
		if msg.id in self._inflight_ids:
			self._inflight_ids.discard(msg.id)
			self.in_flight.labels(msg.task, msg.queue).dec()
			return True
		return False

	def _on_enqueued(self, msg: Message) -> None:
		self.enqueued.labels(msg.task, msg.queue).inc()

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
