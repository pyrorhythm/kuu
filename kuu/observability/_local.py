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
_FORGET_AFTER = timedelta(minutes=10)


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
	def __init__(
		self,
		stale_after: timedelta = _STALE_AFTER,
		forget_after: timedelta = _FORGET_AFTER,
	) -> None:
		self._entries: dict[str, _Entry] = {}
		self._stale_after = stale_after
		self._forget_after = max(forget_after, stale_after)

	def ingest(self, envelope: Envelope) -> None:
		entry = self._entries.get(envelope.instance)
		body = envelope.body
		# stamped on arrival rather than taken from envelope.ts: those clocks belong to
		# different processes, and a backlogged transport would otherwise deliver
		# envelopes that are already older than stale_after, reading as a dead producer
		now = utcnow()

		match body:
			case Hello():
				self._entries[envelope.instance] = _Entry(
					instance_id=envelope.instance,
					hello=body,
					last_state=entry.last_state if entry else None,
					last_seen=now,
				)
			case State() if entry is not None:
				entry.last_state = body
				entry.last_seen = now
			case Bye():
				self._entries.pop(envelope.instance, None)
			case _ if entry is not None:
				entry.last_seen = now
			case _:
				pass

	def get(self, instance_id: str) -> InstanceInfo | None:
		self._forget_expired()
		entry = self._entries.get(instance_id)
		if entry is None or self._is_stale(entry):
			return None
		return entry.snapshot()

	def all(self) -> list[InstanceInfo]:
		self._forget_expired()
		return [e.snapshot() for e in self._entries.values() if not self._is_stale(e)]

	def _is_stale(self, entry: _Entry) -> bool:
		return utcnow() - entry.last_seen > self._stale_after

	def _forget_expired(self) -> None:
		"""Drop entries silent long enough that the producer is certainly gone.

		Going stale only hides an entry — its ``hello`` is kept, so the next ``State``
		brings the instance straight back. Dropping it outright made any gap longer
		than ``stale_after`` permanent: only ``Hello`` can create an entry, and a
		producer emits it rarely.
		"""
		now = utcnow()
		dead = [iid for iid, e in self._entries.items() if now - e.last_seen > self._forget_after]
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
