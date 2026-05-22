from __future__ import annotations

import logging
from collections.abc import Callable
from queue import Empty
from typing import Protocol, TypeVar

import anyio

T = TypeVar("T")


class SyncGetQueue(Protocol[T]):
	def get_nowait(self) -> T: ...


async def drain_sync_queue(
	q: SyncGetQueue[T],
	handler: Callable[[T], None],
	*,
	stop_event: anyio.Event,
	batch_size: int = 100,
	idle_sleep: float = 0.1,
	error_sleep: float = 0.5,
	logger: logging.Logger | None = None,
	error_event: str = "queue_drain.error",
) -> None:
	"""Batch-drain a stdlib :class:`queue.Queue` until ``stop_event`` is set."""
	import anyio.lowlevel

	log = logger or logging.getLogger("kuu.queue_drain")
	while not stop_event.is_set():
		try:
			did_work = False
			for _ in range(batch_size):
				try:
					item = q.get_nowait()
					did_work = True
				except Empty:
					break
				handler(item)
			if did_work:
				await anyio.lowlevel.checkpoint()
			else:
				await anyio.sleep(idle_sleep)
		except Exception as e:
			if stop_event.is_set():
				break
			log.exception("%s error=%s", error_event, e)
			await anyio.sleep(error_sleep)
