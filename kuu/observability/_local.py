from __future__ import annotations

import multiprocessing as mp
import time
from collections.abc import AsyncIterator
from queue import Empty as _QueueEmpty

import anyio

from kuu.observability._protocol import (
	Body,
	Bye,
	Envelope,
	Hello,
	InstanceInfo,
	State,
)

_STALE_AFTER = 5.0


class MpQueueSink:
	"""``EventsSink`` backed by a multiprocessing queue

	intended to be picklable so it can be passed to ``mp.Process`` targets
	"""

	__slots__ = ("_q",)

	def __init__(self, queue: "mp.Queue[Envelope]") -> None:
		self._q = queue

	def emit(self, envelope: Envelope) -> None:
		try:
			self._q.put_nowait(envelope)
		except Exception:
			pass


class MpQueueSource:
	"""async ``EventsSource`` that drains a multiprocessing queue"""

	__slots__ = ("_q", "_poll_interval", "_closed")

	def __init__(self, queue: "mp.Queue[Envelope]", poll_interval: float = 0.1) -> None:
		self._q = queue
		self._poll_interval = poll_interval
		self._closed = False

	def close(self) -> None:
		self._closed = True

	async def __aiter__(self) -> AsyncIterator[Envelope]:
		while not self._closed:
			try:
				while True:
					yield self._q.get_nowait()
			except _QueueEmpty:
				pass
			await anyio.sleep(self._poll_interval)


class InMemoryRegistry:
	"""``InstanceRegistry`` impl with lazy stale eviction

	an entry is considered stale if no envelope of any kind has been
	observed for it within ``stale_after`` seconds; eviction happens on
	read (``get`` / ``all``)
	"""

	def __init__(self, stale_after: float = _STALE_AFTER) -> None:
		self._entries: dict[str, _Entry] = {}
		self._stale_after = stale_after

	def ingest(self, envelope: Envelope) -> None:
		entry = self._entries.get(envelope.instance)
		body: Body = envelope.body

		match body:
			case Hello():
				self._entries[envelope.instance] = _Entry(
					instance_id=envelope.instance,
					hello=body,
					last_state=entry.last_state if entry else None,
					last_seen=envelope.ts,
				)
			case State() if entry is not None:
				entry.last_state = body
				entry.last_seen = envelope.ts
			case Bye():
				self._entries.pop(envelope.instance, None)
			case _ if entry is not None:
				entry.last_seen = envelope.ts
			case _:
				pass

	def get(self, instance_id: str) -> InstanceInfo | None:
		self._evict_stale()
		entry = self._entries.get(instance_id)
		return entry.snapshot() if entry is not None else None

	def all(self) -> list[InstanceInfo]:
		self._evict_stale()
		return [e.snapshot() for e in self._entries.values()]

	def _evict_stale(self) -> None:
		now = time.time()
		dead = [iid for iid, e in self._entries.items() if now - e.last_seen > self._stale_after]
		for iid in dead:
			del self._entries[iid]


class _Entry:
	"""mutable storage for an instance's latest hello / state / last_seen"""

	__slots__ = ("instance_id", "hello", "last_state", "last_seen")

	def __init__(
		self,
		instance_id: str,
		hello: Hello,
		last_state: State | None,
		last_seen: float,
	) -> None:
		self.instance_id = instance_id
		self.hello = hello
		self.last_state = last_state
		self.last_seen = last_seen

	def snapshot(self) -> InstanceInfo:
		return InstanceInfo(
			instance_id=self.instance_id,
			hello=self.hello,
			last_state=self.last_state,
			last_seen=self.last_seen,
		)


__all__ = ["MpQueueSink", "MpQueueSource", "InMemoryRegistry"]
