from __future__ import annotations

import asyncio
import logging
import time as _time
from typing import TYPE_CHECKING, Any

import anyio

from kuu.observability._protocol import Event, EventKind, LogBatch
from kuu.persistence._backend import PersistenceBackend

if TYPE_CHECKING:
	from kuu.config import PersistenceConfig

	from ._rows import RunStatus

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
		self._queue: asyncio.Queue[tuple[str, Any] | None] = asyncio.Queue(maxsize=10_000)
		self._started: dict[str, dict[str, Any]] = {}  # message_id -> start info
		self._run_batch: list[dict[str, Any]] = []
		self._log_batch: list[dict[str, Any]] = []
		self._last_flush = _time.monotonic()
		self._dropped_events = 0
		self._dropped_logs = 0

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
		"""pull every remaining item out of the queue and stage it for flush"""
		while True:
			try:
				item = self._queue.get_nowait()
			except asyncio.QueueEmpty:
				return
			if item is None:
				continue
			kind, payload = item
			if kind == "event":
				self._handle_event(*payload)
			elif kind == "log_batch":
				self._handle_log_batch(*payload)

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
					self._handle_event(*payload)
				elif kind == "log_batch":
					self._handle_log_batch(*payload)
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
		from kuu.persistence._rows import LogRow, RunRow

		run_batch = self._run_batch
		self._run_batch = []
		if run_batch:
			rows = [
				RunRow(
					message_id=r["message_id"],
					attempt=r["attempt"],
					task=r["task"],
					queue=r["queue"],
					instance_id=r["instance_id"],
					worker_pid=r["worker_pid"],
					args_repr=r.get("args_repr"),
					kwargs_repr=r.get("kwargs_repr"),
					started_at=r.get("started_at"),
					finished_at=r.get("finished_at"),
					time_elapsed=r.get("time_elapsed"),
					status=r["status"],
					exc_type=r.get("exc_type"),
					exc_message=r.get("exc_message"),
					traceback=r.get("traceback"),
				)
				for r in run_batch
			]
			try:
				await self._backend.write_runs(rows)
			except Exception:
				log.exception("persistence: write_runs failed, dropped %d rows", len(rows))

		log_batch = self._log_batch
		self._log_batch = []
		if log_batch:
			rows = [
				LogRow(
					message_id=rec["message_id"],
					attempt=rec["attempt"],
					ts=rec["ts"],
					level=rec["level"],
					logger=rec["logger"],
					message=rec["message"],
				)
				for rec in log_batch
			]
			try:
				await self._backend.write_logs(rows)
			except Exception:
				log.exception("persistence: write_logs failed, dropped %d rows", len(rows))

		self._last_flush = _time.monotonic()

		if self._dropped_events or self._dropped_logs:
			log.warning(
				"persistence queue overflow: dropped_events=%d dropped_logs=%d",
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

		if kind == "enqueued" or kind == "started":
			self._started[mid] = {
				"message_id": mid,
				"attempt": evt.attempt or 0,
				"task": evt.task,
				"queue": evt.queue,
				"instance_id": instance_id,
				"worker_pid": evt.worker_pid,
				"started_at": _time.time(),
			}
			return

		finish_ts = _time.time()
		start_info = self._started.pop(mid, None)
		time_elapsed: float | None = None
		if start_info is not None:
			started_at = start_info.get("started_at")
			if started_at is not None:
				time_elapsed = finish_ts - float(started_at)
		else:
			start_info = {
				"message_id": mid,
				"attempt": evt.attempt or 0,
				"task": evt.task,
				"queue": evt.queue,
				"instance_id": instance_id,
				"worker_pid": evt.worker_pid,
			}

		status_map: dict[EventKind, RunStatus] = {
			"succeeded": "succeeded",
			"failed": "failed",
			"retried": "retried",
			"dead": "dead",
		}

		self._run_batch.append(
			{
				**start_info,
				"finished_at": finish_ts,
				"time_elapsed": time_elapsed,
				"status": status_map.get(kind, "failed"),
				"args_repr": evt.args_repr,
				"kwargs_repr": evt.kwargs_repr,
				"exc_type": evt.exc_type,
				"exc_message": evt.exc_message,
				"traceback": evt.traceback,
			}
		)

	def _handle_log_batch(self, instance_id: str, lb: LogBatch) -> None:
		for rec in lb.records:
			self._log_batch.append(
				{
					"message_id": rec.message_id,
					"attempt": rec.attempt,
					"ts": rec.ts,
					"level": rec.level,
					"logger": rec.logger,
					"message": rec.message,
				}
			)

	async def _prune_loop(self, stop_event: anyio.Event) -> None:
		while not stop_event.is_set():
			with anyio.move_on_after(_PRUNE_INTERVAL):
				await stop_event.wait()
			if stop_event.is_set():
				return
			try:
				cutoff = _time.time() - (self._cfg.keep_days * 86400)
				deleted = await self._backend.prune(cutoff)
				if deleted > 0:
					log.info(
						"persistence: pruned %d runs older than %d days",
						deleted,
						self._cfg.keep_days,
					)
			except Exception:
				log.exception("persistence: prune failed")
