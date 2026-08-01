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
from kuu.persistence._rows import LogRow, LogicalRunRow, LogicalRunStatus, PendingRun

if TYPE_CHECKING:
	from kuu.config import PersistenceConfig

log = logging.getLogger("kuu.persistence.worker")

_FLUSH_INTERVAL = 0.2
_FLUSH_BATCH_SIZE = 500
_PRUNE_INTERVAL = 300.0

_LOGICAL_STATUS: dict[EventKind, LogicalRunStatus] = {
	"enqueued": "enqueued",
	"started": "running",
	"failed": "running",
	"retried": "running",
	"dead": "failed",
	"succeeded": "succeeded",
	"cancelled": "cancelled",
}


class PersistenceWorker:
	def __init__(
		self,
		backend: PersistenceBackend,
		cfg: PersistenceConfig,
	) -> None:
		self._backend = backend
		self._cfg = cfg
		self._ready = anyio.Event()
		self._queue: asyncio.Queue[tuple[str, object] | None] = asyncio.Queue(maxsize=10_000)
		self._started: dict[tuple[str, int], PendingRun] = {}
		self._run_batch: list[PendingRun] = []
		self._logical_batch: list[LogicalRunRow] = []
		self._log_batch: list[LogRow] = []
		self._last_flush = _time.monotonic()
		self._progress_written: dict[tuple[str, int], float] = {}
		self._redeliveries: set[tuple[str, int]] = set()
		self._terminal_runs: set[str] = set()
		self._dropped_events = 0
		self._dropped_logs = 0
		self._overflow_gaps: dict[tuple[str, int], int] = {}

	@property
	def backend(self) -> PersistenceBackend:
		return self._backend

	@property
	def ready(self) -> anyio.Event:
		return self._ready

	async def run(self, stop_event: anyio.Event) -> None:
		await self._backend.connect()
		try:
			await self._backend.init_schema()
			self._ready.set()
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
			for record in lb.records:
				key = (record.message_id, record.attempt)
				self._overflow_gaps[key] = self._overflow_gaps.get(key, 0) + 1

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
				or len(self._logical_batch) >= _FLUSH_BATCH_SIZE
				or len(self._log_batch) >= _FLUSH_BATCH_SIZE
				or (
					now - self._last_flush >= _FLUSH_INTERVAL
					and (
						self._run_batch
						or self._logical_batch
						or self._log_batch
						or self._overflow_gaps
					)
				)
			):
				await self._flush()

	async def _flush(self, force: bool = False) -> None:
		del force
		run_batch = self._run_batch
		self._run_batch = []
		redeliveries = self._redeliveries
		self._redeliveries = set()
		if run_batch:
			rows = [pending.to_row() for pending in run_batch]
			try:
				await self._backend.write_runs(rows)
			except Exception as e:
				log.exception(
					"event=persistence.write_runs_failed dropped=%d error=%s", len(rows), e
				)
		for message_id, attempt in redeliveries:
			try:
				await self._backend.mark_previous_attempts_lost(message_id, attempt)
			except Exception as e:
				log.exception(
					"event=persistence.mark_lost_failed message_id=%s attempt=%d error=%s",
					message_id,
					attempt,
					e,
				)

		logical_batch = self._logical_batch
		self._logical_batch = []
		if logical_batch:
			try:
				await self._backend.write_logical_runs(logical_batch)
			except Exception as e:
				log.exception(
					"event=persistence.write_logical_runs_failed dropped=%d error=%s",
					len(logical_batch),
					e,
				)

		log_batch = self._log_batch
		self._log_batch = []
		for (message_id, attempt), dropped in self._overflow_gaps.items():
			log_batch.append(
				LogRow(
					message_id=message_id,
					attempt=attempt,
					ts=utcnow(),
					kind="gap",
					level=30,
					logger="kuu.persistence",
					message="persistence queue overflow",
					dropped=dropped,
				)
			)
		self._overflow_gaps = {}
		if log_batch:
			try:
				await self._backend.write_logs(log_batch)
			except Exception as e:
				log.exception(
					"event=persistence.write_logs_failed dropped=%d error=%s", len(log_batch), e
				)

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
		if mid is None or mid in self._terminal_runs:
			return

		kind: EventKind = evt.kind
		event_ts = (
			evt.ts
			if isinstance(evt.ts, datetime)
			else datetime.fromtimestamp(evt.ts, tz=timezone.utc)
		)

		attempt = evt.attempt or 0
		self._logical_batch.append(
			LogicalRunRow(
				message_id=mid,
				task=evt.task,
				queue=evt.queue,
				instance_id=instance_id,
				status=_LOGICAL_STATUS[kind],
				created_at=event_ts,
				updated_at=event_ts,
				replay_of=evt.replay_of,
				attempt_count=attempt + 1,
				dead_lettered=kind == "dead",
			)
		)
		key = (mid, attempt)

		if kind == "enqueued" or kind == "started":
			status = "enqueued" if kind == "enqueued" else "started"
			if kind == "started" and attempt > 0:
				self._redeliveries.add((mid, attempt))
			started = PendingRun(
				message_id=mid,
				attempt=attempt,
				task=evt.task,
				queue=evt.queue,
				instance_id=instance_id,
				worker_pid=evt.worker_pid,
				args=evt.args,
				kwargs=evt.kwargs,
				headers=evt.headers,
				input_complete=evt.input_complete,
				started_at=event_ts,
				status=status,
			)
			self._started[key] = started
			self._run_batch.append(started)
			return

		finish_ts = event_ts
		start_info = self._started.pop(key, None)
		if start_info is None:
			start_info = PendingRun(
				message_id=mid,
				attempt=attempt,
				task=evt.task,
				queue=evt.queue,
				instance_id=instance_id,
				worker_pid=evt.worker_pid,
				args=evt.args,
				kwargs=evt.kwargs,
				headers=evt.headers,
				input_complete=evt.input_complete,
			)

		self._run_batch.append(
			start_info.finish(
				kind=kind,
				finish_ts=finish_ts,
				args=evt.args,
				kwargs=evt.kwargs,
				headers=evt.headers,
				input_complete=evt.input_complete,
				result_preview=evt.result_preview,
				exc_type=evt.exc_type,
				exc_message=evt.exc_message,
				traceback=evt.traceback,
				failure=evt.failure,
			)
		)
		if kind in {"succeeded", "dead", "cancelled"}:
			self._terminal_runs.add(mid)
		self._progress_written.pop(key, None)

	def _handle_log_batch(self, instance_id: str, lb: LogBatch) -> None:
		del instance_id
		for rec in lb.records:
			key = (rec.message_id, rec.attempt)
			if rec.kind == "progress" and not rec.fields.get("final"):
				last = self._progress_written.get(key, 0.0)
				if rec.ts - last < 1.0:
					continue
				self._progress_written[key] = rec.ts
			self._log_batch.append(
				LogRow(
					message_id=rec.message_id,
					attempt=rec.attempt,
					ts=datetime.fromtimestamp(rec.ts, tz=timezone.utc),
					kind=rec.kind,
					seq=rec.seq,
					level=rec.level,
					logger=rec.logger,
					message=rec.message,
					fields=rec.fields,
					current=rec.current,
					total=rec.total,
					dropped=rec.dropped,
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
				deleted = await self._backend.prune(cutoff, self._cfg.max_runs)
				if deleted > 0:
					log.info(
						"event=persistence.pruned deleted=%d keep_days=%d",
						deleted,
						self._cfg.keep_days,
					)
			except Exception as e:
				log.exception("event=persistence.prune_failed error=%s", e)
