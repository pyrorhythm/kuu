from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import anyio

from kuu._util import utcnow
from kuu.observability._protocol import Event, EventKind, LogBatch
from kuu.persistence._backend import PersistenceBackend
from kuu.persistence._rows import LogRow, PendingRun

if TYPE_CHECKING:
	from kuu.config import PersistenceConfig

log = logging.getLogger("kuu.persistence.worker")

_FLUSH_INTERVAL = 0.2
_FLUSH_BATCH_SIZE = 500
_PRUNE_INTERVAL = 300.0


class PersistenceWorker:
	def __init__(
		self,
		backend: PersistenceBackend,
		cfg: PersistenceConfig,
	) -> None:
		self._backend = backend
		self._cfg = cfg
		self._queue: asyncio.Queue[tuple[str, object] | None] = asyncio.Queue(maxsize=10_000)
		self._started: dict[tuple[str, int], PendingRun] = {}
		self._run_batch: list[PendingRun] = []
		self._log_batch: list[LogRow] = []
		self._last_flush = _time.monotonic()
		self._dropped_events = 0
		self._dropped_logs = 0

	@property
	def backend(self) -> PersistenceBackend:
		return self._backend

	async def run(self, stop_event: anyio.Event) -> None:
		await self._backend.connect()
		try:
			await self._backend.init_schema()
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._drain_loop, stop_event)
				tg.start_soon(self._prune_loop, stop_event)
				await stop_event.wait()
				tg.cancel_scope.cancel()
		finally:
			with anyio.move_on_after(5.0):
				self._drain_queue_nowait()
				await self._flush(force=True)
			await self._backend.close()

	def _drain_queue_nowait(self) -> None:
		while True:
			try:
				item = self._queue.get_nowait()
			except asyncio.QueueEmpty:
				return
			if item is None:
				continue
			kind, payload = item
			if kind == "event":
				self._handle_event(*payload)  # type: ignore[arg-type]
			elif kind == "log_batch":
				self._handle_log_batch(*payload)  # type: ignore[arg-type]

	def enqueue_event(self, instance_id: str, evt: Event) -> None:
		try:
			self._queue.put_nowait(("event", (instance_id, evt)))
		except asyncio.QueueFull:
			self._dropped_events += 1

	def enqueue_log_batch(self, instance_id: str, lb: LogBatch) -> None:
		try:
			self._queue.put_nowait(("log_batch", (instance_id, lb)))
		except asyncio.QueueFull:
			self._dropped_logs += len(lb.records)

	async def _drain_loop(self, stop_event: anyio.Event) -> None:
		while not stop_event.is_set():
			try:
				item = await asyncio.wait_for(self._queue.get(), timeout=0.1)
				if item is None:
					continue
				kind, payload = item
				if kind == "event":
					self._handle_event(*payload)  # type: ignore[arg-type]
				elif kind == "log_batch":
					self._handle_log_batch(*payload)  # type: ignore[arg-type]
			except asyncio.TimeoutError:
				pass

			now = _time.monotonic()
			if (
				len(self._run_batch) >= _FLUSH_BATCH_SIZE
				or len(self._log_batch) >= _FLUSH_BATCH_SIZE
				or (
					now - self._last_flush >= _FLUSH_INTERVAL
					and (self._run_batch or self._log_batch)
				)
			):
				await self._flush()

	async def _flush(self, force: bool = False) -> None:
		run_batch = self._run_batch
		self._run_batch = []
		if run_batch:
			rows = [pending.to_row() for pending in run_batch]
			try:
				await self._backend.write_runs(rows)
			except Exception as e:
				log.exception("event=persistence.write_runs_failed dropped=%d error=%s", len(rows), e)

		log_batch = self._log_batch
		self._log_batch = []
		if log_batch:
			try:
				await self._backend.write_logs(log_batch)
			except Exception as e:
				log.exception("event=persistence.write_logs_failed dropped=%d error=%s", len(log_batch), e)

		self._last_flush = _time.monotonic()

		if self._dropped_events or self._dropped_logs:
			log.warning(
				"event=persistence.queue_overflow dropped_events=%d dropped_logs=%d",
				self._dropped_events,
				self._dropped_logs,
			)
			self._dropped_events = 0
			self._dropped_logs = 0

	def _handle_event(self, instance_id: str, evt: Event) -> None:
		mid = evt.message_id
		if mid is None:
			return

		kind: EventKind = evt.kind

		attempt = evt.attempt or 0
		key = (mid, attempt)

		if kind == "enqueued" or kind == "started":
			status = "enqueued" if kind == "enqueued" else "started"
			started = PendingRun(
				message_id=mid,
				attempt=attempt,
				task=evt.task,
				queue=evt.queue,
				instance_id=instance_id,
				worker_pid=evt.worker_pid,
				started_at=utcnow(),
				status=status,
			)
			self._started[key] = started
			self._run_batch.append(started)
			return

		finish_ts = utcnow()
		start_info = self._started.pop(key, None)
		if start_info is None:
			start_info = PendingRun(
				message_id=mid,
				attempt=attempt,
				task=evt.task,
				queue=evt.queue,
				instance_id=instance_id,
				worker_pid=evt.worker_pid,
			)

		self._run_batch.append(
			start_info.finish(
				kind=kind,
				finish_ts=finish_ts,
				args=evt.args,
				kwargs=evt.kwargs,
				exc_type=evt.exc_type,
				exc_message=evt.exc_message,
				traceback=evt.traceback,
			)
		)

	def _handle_log_batch(self, instance_id: str, lb: LogBatch) -> None:
		del instance_id
		for rec in lb.records:
			self._log_batch.append(
				LogRow(
					message_id=rec.message_id,
					attempt=rec.attempt,
					ts=datetime.fromtimestamp(rec.ts, tz=timezone.utc),
					level=rec.level,
					logger=rec.logger,
					message=rec.message,
				)
			)

	async def _prune_loop(self, stop_event: anyio.Event) -> None:
		while not stop_event.is_set():
			with anyio.move_on_after(_PRUNE_INTERVAL):
				await stop_event.wait()
			if stop_event.is_set():
				return
			try:
				cutoff = utcnow() - timedelta(days=self._cfg.keep_days)
				deleted = await self._backend.prune(cutoff)
				if deleted > 0:
					log.info(
						"event=persistence.pruned deleted=%d keep_days=%d",
						deleted,
						self._cfg.keep_days,
					)
			except Exception as e:
				log.exception("event=persistence.prune_failed error=%s", e)
