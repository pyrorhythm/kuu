from __future__ import annotations

import contextvars
import logging
import sys
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TextIO

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


@dataclass(slots=True)
class _AttemptState:
	seq: int = 0
	bytes: int = 0
	dropped: int = 0
	latest_progress: tuple[float, float | None, str | None, dict[str, Any]] | None = None


_buffer: list[Any] = []
_buffer_lock = threading.Lock()
_attempts: dict[tuple[str, int], _AttemptState] = {}
_queue: "mp.Queue[Any] | None" = None
_flush_threshold = 100
_flush_thread: threading.Thread | None = None
_flush_stop = threading.Event()
_max_attempt_bytes = 10 * 1024 * 1024
_handler: TaskLogHandler | None = None
_original_stdout: TextIO | None = None
_original_stderr: TextIO | None = None

_STANDARD_LOG_FIELDS = frozenset(logging.makeLogRecord({}).__dict__) | {
	"asctime",
	"message",
}
_MAX_FIELD_DEPTH = 4
_MAX_FIELDS = 50
_MAX_VALUE_LENGTH = 4096


def _json_safe(value: Any, depth: int = 0) -> Any:
	if value is None or isinstance(value, bool | int | float):
		return value
	if isinstance(value, str):
		return value[:_MAX_VALUE_LENGTH]
	if depth >= _MAX_FIELD_DEPTH:
		return "<max-depth>"
	if isinstance(value, dict):
		return {
			str(key)[:128]: _json_safe(item, depth + 1)
			for key, item in list(value.items())[:_MAX_FIELDS]
		}
	if isinstance(value, list | tuple | set | frozenset):
		return [_json_safe(item, depth + 1) for item in list(value)[:_MAX_FIELDS]]
	try:
		return repr(value)[:_MAX_VALUE_LENGTH]
	except Exception:
		return "<unrepresentable>"


def _trace_fields() -> dict[str, str]:
	try:
		from opentelemetry.trace import get_current_span

		context = get_current_span().get_span_context()
		if context.is_valid:
			return {
				"trace_id": f"{context.trace_id:032x}",
				"span_id": f"{context.span_id:016x}",
			}
	except Exception:
		pass
	return {}


def _log_fields(record: logging.LogRecord) -> dict[str, Any]:
	return {
		key: _json_safe(value)
		for key, value in record.__dict__.items()
		if key not in _STANDARD_LOG_FIELDS and not key.startswith("_")
	}


def _append(
	*,
	msg: "Message",
	kind: str,
	level: int,
	logger: str,
	message: str,
	fields: dict[str, Any] | None = None,
	current: float | None = None,
	total: float | None = None,
	dropped: int = 0,
	force: bool = False,
) -> None:
	from kuu.observability._protocol import LogRecord

	key = (str(msg.id), msg.attempt)
	message = message[:_MAX_VALUE_LENGTH]
	fields = {**(fields or {}), **_trace_fields()}
	size = len(message.encode(errors="replace")) + len(repr(fields).encode(errors="replace"))
	flush_now = False
	with _buffer_lock:
		state = _attempts.setdefault(key, _AttemptState())
		state.seq += 1
		if not force and state.bytes + size > _max_attempt_bytes:
			state.dropped += 1
			return
		state.bytes += size
		_buffer.append(
			LogRecord(
				message_id=key[0],
				attempt=key[1],
				kind=kind,  # type: ignore[arg-type]
				seq=state.seq,
				level=level,
				logger=logger,
				message=message,
				fields=fields,
				current=current,
				total=total,
				dropped=dropped,
				ts=time.time(),
			)
		)
		flush_now = len(_buffer) >= _flush_threshold
	if flush_now:
		flush()


class TaskLogHandler(logging.Handler):
	"""Capture stdlib log records emitted while a task Attempt is in context."""

	def emit(self, record: logging.LogRecord) -> None:
		if record.name.startswith("kuu.persistence") or record.name.startswith("kuu.orchestrator"):
			return
		msg = _current_msg.get()
		if msg is None:
			return
		try:
			formatted = self.format(record)
		except Exception:
			return
		_append(
			msg=msg,
			kind="log",
			level=record.levelno,
			logger=record.name,
			message=formatted,
			fields=_log_fields(record),
		)


class _TaskStream:
	def __init__(self, wrapped: TextIO, kind: str, level: int) -> None:
		self._wrapped = wrapped
		self._kind = kind
		self._level = level

	def write(self, value: str) -> int:
		written = self._wrapped.write(value)
		msg = _current_msg.get()
		if msg is not None:
			for line in value.splitlines():
				if line.strip():
					_append(
						msg=msg,
						kind=self._kind,
						level=self._level,
						logger=self._kind,
						message=line,
					)
		return written

	def flush(self) -> None:
		self._wrapped.flush()

	def fileno(self) -> int:
		return self._wrapped.fileno()

	def __getattr__(self, name: str) -> Any:
		return getattr(self._wrapped, name)


def progress(
	current: int | float,
	total: int | float | None = None,
	message: str | None = None,
	**fields: Any,
) -> None:
	"""Publish a non-blocking structured progress update for the current Attempt."""
	msg = _current_msg.get()
	if msg is None:
		return
	current_value = float(current)
	total_value = float(total) if total is not None else None
	safe_fields = _json_safe(fields)
	safe_message = message[:_MAX_VALUE_LENGTH] if message is not None else None
	key = (str(msg.id), msg.attempt)
	with _buffer_lock:
		state = _attempts.setdefault(key, _AttemptState())
		state.latest_progress = (current_value, total_value, safe_message, safe_fields)
	_append(
		msg=msg,
		kind="progress",
		level=0,
		logger="kuu.progress",
		message=safe_message or "",
		fields=safe_fields,
		current=current_value,
		total=total_value,
	)


def finish_attempt(msg: "Message") -> None:
	"""Emit final progress/gap records and forget the Attempt's capture budget."""
	key = (str(msg.id), msg.attempt)
	with _buffer_lock:
		state = _attempts.get(key)
	if state is None:
		return
	if state.latest_progress is not None:
		current, total, message, fields = state.latest_progress
		_append(
			msg=msg,
			kind="progress",
			level=0,
			logger="kuu.progress",
			message=message or "",
			fields={**fields, "final": True},
			current=current,
			total=total,
			force=True,
		)
	if state.dropped:
		_append(
			msg=msg,
			kind="gap",
			level=logging.WARNING,
			logger="kuu.observability",
			message=f"{state.dropped} observation records dropped",
			fields={"dropped": state.dropped},
			dropped=state.dropped,
			force=True,
		)
	with _buffer_lock:
		_attempts.pop(key, None)
	flush()


def flush() -> None:
	"""Flush the observation buffer onto the worker-to-supervisor queue."""
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


def install(
	queue: "mp.Queue[Any]",
	level: int = logging.INFO,
	interval: float = 0.2,
	max_attempt_bytes: int = 10 * 1024 * 1024,
) -> None:
	"""Install task logging/stdout capture and start the flusher thread."""
	global _queue, _flush_thread, _max_attempt_bytes, _handler
	global _original_stdout, _original_stderr
	_queue = queue
	_max_attempt_bytes = max_attempt_bytes
	if _handler is None:
		_handler = TaskLogHandler(level=level)
		_handler.setFormatter(
			logging.Formatter(
				"%(asctime)s [%(levelname)s] %(name)s: %(message)s",
				datefmt="%Y-%m-%dT%H:%M:%S",
			)
		)
		root = logging.getLogger()
		if root.level > level or root.level == logging.NOTSET:
			root.setLevel(level)
		root.addHandler(_handler)
	if _original_stdout is None:
		_original_stdout = sys.stdout
		sys.stdout = _TaskStream(sys.stdout, "stdout", logging.INFO)  # type: ignore[assignment]
	if _original_stderr is None:
		_original_stderr = sys.stderr
		sys.stderr = _TaskStream(sys.stderr, "stderr", logging.ERROR)  # type: ignore[assignment]
	_flush_stop.clear()
	if _flush_thread is None or not _flush_thread.is_alive():
		_flush_thread = threading.Thread(
			target=_flush_loop, args=(interval,), name="kuu-log-flusher", daemon=True
		)
		_flush_thread.start()


def shutdown() -> None:
	"""Stop capture, restore process streams, and drain remaining records."""
	global _queue, _flush_thread, _handler, _original_stdout, _original_stderr
	_flush_stop.set()
	if _flush_thread is not None:
		_flush_thread.join(timeout=2.0)
	flush()
	if _handler is not None:
		logging.getLogger().removeHandler(_handler)
	if _original_stdout is not None:
		sys.stdout = _original_stdout
	if _original_stderr is not None:
		sys.stderr = _original_stderr
	with _buffer_lock:
		_attempts.clear()
		_buffer.clear()
	_queue = None
	_flush_thread = None
	_handler = None
	_original_stdout = None
	_original_stderr = None


def _flush_loop(interval: float) -> None:
	while not _flush_stop.wait(interval):
		flush()


__all__ = [
	"TaskLogHandler",
	"set_current_msg",
	"reset_current_msg",
	"current_msg",
	"progress",
	"finish_attempt",
	"install",
	"shutdown",
	"flush",
]
