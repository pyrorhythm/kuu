from __future__ import annotations

import contextvars
import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
	import multiprocessing as mp

	from kuu.message import Message


_current_msg: contextvars.ContextVar["Message | None"] = contextvars.ContextVar(
	"kuu_current_msg", default=None
)


def set_current_msg(msg: "Message") -> contextvars.Token:
	return _current_msg.set(msg)


def reset_current_msg(token: contextvars.Token) -> None:
	_current_msg.reset(token)


def current_msg() -> "Message | None":
	return _current_msg.get()


_buffer: list[Any] = []
_buffer_lock = threading.Lock()
_queue: "mp.Queue[Any] | None" = None
_flush_threshold = 100
_flush_thread: threading.Thread | None = None
_flush_stop = threading.Event()


def _make_record(record: logging.LogRecord, msg_id: str, attempt: int, formatted: str) -> Any:
	from kuu.observability._protocol import LogRecord as _LR

	return _LR(
		message_id=msg_id,
		attempt=attempt,
		level=record.levelno,
		logger=record.name,
		message=formatted,
		ts=record.created,
	)


class TaskLogHandler(logging.Handler):
	"""capture log records emitted while a task message is in context

	pushes :class:`kuu.observability.LogRecord` objects onto a shared buffer;
	the buffer is flushed to the supervisor's mp.Queue either when full or by
	a daemon flusher thread on a fixed cadence
	"""

	def emit(self, record: logging.LogRecord) -> None:
		if record.name.startswith("kuu.persistence") or record.name.startswith(
			"kuu.orchestrator"
		):
			return
		msg = _current_msg.get()
		if msg is None:
			return
		try:
			formatted = self.format(record)
		except Exception:
			return
		entry = _make_record(record, str(msg.id), msg.attempt, formatted)
		flush_now = False
		with _buffer_lock:
			_buffer.append(entry)
			if len(_buffer) >= _flush_threshold:
				flush_now = True
		if flush_now:
			flush()


def flush() -> None:
	"""flush the log buffer onto the worker→supervisor mp.Queue"""
	from kuu.observability._protocol import LogBatch

	q = _queue
	if q is None:
		return
	with _buffer_lock:
		if not _buffer:
			return
		batch = list(_buffer)
		_buffer.clear()
	try:
		q.put_nowait(LogBatch(records=batch))
	except Exception:
		pass


def install(queue: "mp.Queue[Any]", level: int = logging.INFO, interval: float = 0.2) -> None:
	"""install the capture handler on the root logger and start the flusher thread"""
	global _queue, _flush_thread
	_queue = queue
	handler = TaskLogHandler(level=level)
	handler.setFormatter(
		logging.Formatter(
			"%(asctime)s [%(levelname)s] %(name)s: %(message)s",
			datefmt="%Y-%m-%dT%H:%M:%S",
		)
	)
	root = logging.getLogger()
	if root.level > level or root.level == logging.NOTSET:
		root.setLevel(level)
	root.addHandler(handler)
	_flush_stop.clear()
	t = threading.Thread(
		target=_flush_loop, args=(interval,), name="kuu-log-flusher", daemon=True
	)
	t.start()
	_flush_thread = t


def shutdown() -> None:
	"""stop the flusher thread and drain remaining records"""
	_flush_stop.set()
	t = _flush_thread
	if t is not None:
		t.join(timeout=2.0)
	flush()


def _flush_loop(interval: float) -> None:
	while not _flush_stop.wait(interval):
		flush()


__all__ = [
	"TaskLogHandler",
	"set_current_msg",
	"reset_current_msg",
	"current_msg",
	"install",
	"shutdown",
	"flush",
]
