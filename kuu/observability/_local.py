from __future__ import annotations

import multiprocessing as mp
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from queue import Empty as _QueueEmpty

import anyio
import anyio.lowlevel

from kuu._util import utcnow
from kuu.observability._protocol import (
	Bye,
	Envelope,
	Hello,
	InstanceInfo,
	State,
)

_STALE_AFTER = timedelta(seconds=5.0)


class MpQueueSink:
	__slots__ = ("_q",)

	def __init__(self, queue: "mp.Queue[Envelope]") -> None:
		self._q = queue

	def emit(self, envelope: Envelope) -> None:
		try:
			self._q.put_nowait(envelope)
		except Exception:
			pass


class MpQueueSource:
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
				did_work = False
				for _ in range(100):
					try:
						yield self._q.get_nowait()
						did_work = True
					except _QueueEmpty:
						break
				if did_work:
					await anyio.lowlevel.checkpoint()
				else:
					await anyio.sleep(self._poll_interval)
			except Exception:
				if self._closed:
					break
				raise


class InMemoryRegistry:
	def __init__(self, stale_after: timedelta = _STALE_AFTER) -> None:
		self._entries: dict[str, _Entry] = {}
		self._stale_after = stale_after

	def ingest(self, envelope: Envelope) -> None:
		entry = self._entries.get(envelope.instance)
		body = envelope.body

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
		now = utcnow()
		dead = [iid for iid, e in self._entries.items() if now - e.last_seen > self._stale_after]
		for iid in dead:
			del self._entries[iid]


class _Entry:
	__slots__ = ("instance_id", "hello", "last_state", "last_seen")

	def __init__(
		self,
		instance_id: str,
		hello: Hello,
		last_state: State | None,
		last_seen: datetime,
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
