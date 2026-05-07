from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from kuu._util import utcnow

if TYPE_CHECKING:
	from kuu.app import Kuu
	from kuu.message import Message


@dataclass(slots=True)
class EventRecord:
	ts: datetime
	task: str
	event: str


class StatsCollector:
	def __init__(
		self,
		app: Kuu | None = None,
		max_events: int = 20000,
		*,
		connect_app_events: bool = True,
	) -> None:
		self.app = app
		self.events_log: deque[EventRecord] = deque(maxlen=max_events)
		self.totals = Counter()

		if connect_app_events and app is not None:
			app.events.task_enqueued.connect(self._on_enqueued)
			app.events.task_succeeded.connect(self._on_succeeded)
			app.events.task_failed.connect(self._on_failed)
			app.events.task_retried.connect(self._on_retried)
			app.events.task_dead.connect(self._on_dead)

	def _bump(self, event: str, msg: Message) -> None:
		self.totals[event] += 1
		self.events_log.append(EventRecord(utcnow(), msg.task, event))

	def _on_enqueued(self, msg: Message) -> None:
		self._bump("enqueued", msg)

	def _on_succeeded(self, msg: Message, elapsed: float) -> None:
		self._bump("succeeded", msg)

	def _on_failed(self, msg: Message, exc: Exception) -> None:
		self._bump("failed", msg)

	def _on_retried(self, msg: Message, delay: float) -> None:
		self._bump("retried", msg)

	def _on_dead(self, msg: Message) -> None:
		self._bump("dead", msg)

	def ingest(self, event: str, task: str, ts: datetime) -> None:
		self.totals[event] += 1
		self.events_log.append(EventRecord(ts, task, event))

	def activity_series(
		self, buckets: int = 60, bucket_sec: timedelta = timedelta(seconds=5)
	) -> dict:
		out: dict = {
			"times": [],
			"enqueued": [],
			"succeeded": [],
			"failed": [],
			"retried": [],
			"dead": [],
		}
		if not self.events_log:
			return out
		now = utcnow()
		start = now - buckets * bucket_sec
		window = [e for e in self.events_log if e.ts >= start]
		for i in range(buckets):
			t0 = start + i * bucket_sec
			t1 = t0 + bucket_sec
			out["times"].append(t1.isoformat())
			for k in ("enqueued", "succeeded", "failed", "retried", "dead"):
				out[k].append(sum(1 for e in window if t0 <= e.ts < t1 and e.event == k))
		return out
