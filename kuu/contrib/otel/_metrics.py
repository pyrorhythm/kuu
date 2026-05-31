from __future__ import annotations

import typing
from typing import Any

from opentelemetry import metrics

if typing.TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.message import Message


class OtelMetrics:
	"""
	Wires OTEL metrics onto Kuu task lifecycle events.

	Instruments created (all under ``meter_name`` scope):
		- ``kuu.task.enqueued`` (Counter) : tasks enqueued
		- ``kuu.task.processed`` (Counter) : tasks processed, with ``status`` label
		- ``kuu.task.duration`` (Histogram, seconds) : task execution duration
		- ``kuu.task.in_flight`` (UpDownCounter) : tasks currently executing
		- ``kuu.task.retried`` (Counter) : retry attempts
	"""

	def __init__(
		self,
		*,
		app: Kuu,
		meter_name: str = "kuu",
	):
		self._app = app
		self._meter = metrics.get_meter(meter_name)

		self._enqueued = self._meter.create_counter(
			"kuu.task.enqueued",
			"tasks",
			"Tasks enqueued",
		)
		self._processed = self._meter.create_counter(
			"kuu.task.processed",
			"tasks",
			"Tasks processed by outcome",
		)
		self._duration = self._meter.create_histogram(
			"kuu.task.duration",
			"s",
			"Task execution duration",
		)
		self._in_flight = self._meter.create_up_down_counter(
			"kuu.task.in_flight",
			"tasks",
			"Tasks currently executing",
		)
		self._retried = self._meter.create_counter(
			"kuu.task.retried",
			"tasks",
			"Task retry attempts",
		)

		self._inflight_ids = set()

		ev = app.events
		ev.task_enqueued.connect(self._on_enqueued)
		ev.task_started.connect(self._on_started)
		ev.task_succeeded.connect(self._on_succeeded)
		ev.task_failed.connect(self._on_failed)
		ev.task_retried.connect(self._on_retried)
		ev.task_dead.connect(self._on_dead)

	def _attrs(self, msg: Message, **extra: str) -> dict[str, Any]:
		base: dict[str, Any] = {"task.name": msg.task, "queue": msg.queue}
		base.update(extra)
		return base

	def _settle(self, msg: Message) -> None:
		if msg.id in self._inflight_ids:
			self._inflight_ids.discard(msg.id)
			self._in_flight.add(-1, self._attrs(msg))

	def _on_enqueued(self, msg: Message) -> None:
		self._enqueued.add(1, self._attrs(msg))

	def _on_started(self, msg: Message) -> None:
		self._inflight_ids.add(msg.id)
		self._in_flight.add(1, self._attrs(msg))

	def _on_succeeded(self, msg: Message, elapsed: float) -> None:
		self._duration.record(elapsed, self._attrs(msg))
		self._processed.add(1, self._attrs(msg, status="ok"))
		self._settle(msg)

	def _on_failed(self, msg: Message, exc: BaseException) -> None:
		self._processed.add(1, self._attrs(msg, status="error"))
		self._settle(msg)

	def _on_retried(self, msg: Message, delay: float) -> None:
		self._retried.add(1, self._attrs(msg))
		self._settle(msg)

	def _on_dead(self, msg: Message) -> None:
		self._processed.add(1, self._attrs(msg, status="dead"))
		self._settle(msg)

	def disconnect(self) -> None:
		ev = self._app.events
		ev.task_enqueued.disconnect(self._on_enqueued)
		ev.task_started.disconnect(self._on_started)
		ev.task_succeeded.disconnect(self._on_succeeded)
		ev.task_failed.disconnect(self._on_failed)
		ev.task_retried.disconnect(self._on_retried)
		ev.task_dead.disconnect(self._on_dead)


__all__ = ("OtelMetrics",)
