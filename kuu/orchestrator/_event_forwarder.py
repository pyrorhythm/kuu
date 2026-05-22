from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime as dtime
from datetime import timezone as tz
from queue import Queue

import anyio

from kuu._queue_drain import drain_sync_queue
from kuu.observability import PROTOCOL_VERSION, Envelope, Event, EventsSink
from kuu.observability._protocol import LogBatch

log = logging.getLogger("kuu.orchestrator.event_forwarder")


class EventForwarder:
	def __init__(
		self,
		*,
		instance_id: str,
		events_sink: EventsSink,
		events_queue: Queue,
	) -> None:
		self._instance_id = instance_id
		self._sink = events_sink
		self._queue = events_queue
		self.inflight: Counter[str] = Counter()
		self.current_task: dict[int, str] = {}

	def _emit(self, body: object) -> None:
		try:
			self._sink.emit(
				Envelope(
					v=PROTOCOL_VERSION,
					instance=self._instance_id,
					ts=dtime.now(tz=tz.utc),
					body=body,
				)
			)
		except Exception as e:
			log.exception("event=event_forwarder.emit_failed error=%s", e)

	def _emit_worker_event(self, we: Event) -> None:
		try:
			self._sink.emit(
				Envelope(
					v=PROTOCOL_VERSION,
					instance=self._instance_id,
					ts=we.ts,
					body=we,
				)
			)
		except Exception as e:
			log.exception("event=event_forwarder.worker_event_failed error=%s", e)

	def _handle_item(self, item: object) -> None:
		if isinstance(item, LogBatch):
			self._emit(item)
			return
		we: Event = item
		if we.kind == "started":
			self.inflight[we.queue] += 1
			self.current_task[we.worker_pid] = we.task
			return
		if self.inflight[we.queue] > 0:
			self.inflight[we.queue] -= 1
		self.current_task.pop(we.worker_pid, None)
		self._emit_worker_event(we)

	async def run(self, stop_event: anyio.Event) -> None:
		await drain_sync_queue(
			self._queue,
			self._handle_item,
			stop_event=stop_event,
			batch_size=100,
			idle_sleep=0.1,
			error_sleep=0.5,
			logger=log,
			error_event="event=event_forwarder.error",
		)
