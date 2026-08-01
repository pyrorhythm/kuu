from __future__ import annotations

import linecache
import sys
import traceback
from types import TracebackType
from typing import Literal

from msgspec import Struct

_MAX_CHAIN_DEPTH = 5
_MAX_FRAMES = 50
_MAX_MESSAGE_CHARS = 4096
_MAX_NAME_CHARS = 512
_MAX_PATH_CHARS = 2048
_MAX_SOURCE_LINE_CHARS = 500
_MAX_TRACEBACK_CHARS = 64 * 1024
_SOURCE_CONTEXT_LINES = 2


class RemoteStackFrame(Struct, frozen=True):
	filename: str
	lineno: int
	function: str
	source_start_line: int
	source: tuple[str, ...]
	application: bool = True


class RemoteFailure(Struct, frozen=True):
	type_name: str
	type_module: str
	message: str
	frames: tuple[RemoteStackFrame, ...]
	traceback: str
	cause: RemoteFailure | None = None
	context: RemoteFailure | None = None


def _bounded(value: str, limit: int) -> str:
	return value if len(value) <= limit else value[:limit]


def _message(exc: BaseException) -> str:
	try:
		return _bounded(str(exc), _MAX_MESSAGE_CHARS)
	except BaseException:  # noqa: BLE001 - failure capture must not mask the task failure
		return "<exception message unavailable>"


def _frames(tb: TracebackType | None) -> tuple[RemoteStackFrame, ...]:
	items: list[tuple[str, int, str]] = []
	while tb is not None:
		code = tb.tb_frame.f_code
		items.append((code.co_filename, tb.tb_lineno, code.co_name))
		tb = tb.tb_next

	captured: list[RemoteStackFrame] = []
	for filename, lineno, function in items[-_MAX_FRAMES:]:
		start = max(1, lineno - _SOURCE_CONTEXT_LINES)
		end = lineno + _SOURCE_CONTEXT_LINES
		source_lines = (linecache.getline(filename, line) for line in range(start, end + 1))
		source = tuple(
			_bounded(line.rstrip("\n"), _MAX_SOURCE_LINE_CHARS) for line in source_lines if line
		)
		normalized = filename.replace("\\", "/")
		stdlib = sys.base_prefix.replace("\\", "/") + "/lib/python"
		captured.append(
			RemoteStackFrame(
				filename=_bounded(filename, _MAX_PATH_CHARS),
				lineno=lineno,
				function=_bounded(function, _MAX_NAME_CHARS),
				source_start_line=start,
				source=source,
				application=(
					"/site-packages/" not in normalized
					and "/dist-packages/" not in normalized
					and not normalized.startswith(stdlib)
				),
			)
		)
	return tuple(captured)


def _formatted_traceback(exc: BaseException, type_name: str, message: str) -> str:
	try:
		formatted = "".join(
			traceback.format_exception(type(exc), exc, exc.__traceback__, limit=-_MAX_FRAMES)
		)
	except BaseException:  # noqa: BLE001 - failure capture must not mask the task failure
		formatted = f"{type_name}: {message}\n"
	return _bounded(formatted, _MAX_TRACEBACK_CHARS)


def sanitize_remote_failure(failure: RemoteFailure, depth: int = 0) -> RemoteFailure:
	"""Apply collector-side bounds to inert failure data received from outside."""
	cause = context = None
	if depth < _MAX_CHAIN_DEPTH:
		if failure.cause is not None:
			cause = sanitize_remote_failure(failure.cause, depth + 1)
		elif failure.context is not None:
			context = sanitize_remote_failure(failure.context, depth + 1)
	frames = tuple(
		RemoteStackFrame(
			filename=_bounded(frame.filename, _MAX_PATH_CHARS),
			lineno=max(0, frame.lineno),
			function=_bounded(frame.function, _MAX_NAME_CHARS),
			source_start_line=max(0, frame.source_start_line),
			source=tuple(
				_bounded(line, _MAX_SOURCE_LINE_CHARS)
				for line in frame.source[: 2 * _SOURCE_CONTEXT_LINES + 1]
			),
			application=frame.application,
		)
		for frame in failure.frames[-_MAX_FRAMES:]
	)
	return RemoteFailure(
		type_name=_bounded(failure.type_name, _MAX_NAME_CHARS),
		type_module=_bounded(failure.type_module, _MAX_NAME_CHARS),
		message=_bounded(failure.message, _MAX_MESSAGE_CHARS),
		frames=frames,
		traceback=_bounded(failure.traceback, _MAX_TRACEBACK_CHARS),
		cause=cause,
		context=context,
	)


def capture_remote_failure(exc: BaseException) -> RemoteFailure:
	def capture(current: BaseException, depth: int, seen: set[int]) -> RemoteFailure:
		cause = context = None
		if depth < _MAX_CHAIN_DEPTH and id(current) not in seen:
			seen.add(id(current))
			if current.__cause__ is not None:
				cause = capture(current.__cause__, depth + 1, seen)
			elif current.__context__ is not None and not current.__suppress_context__:
				context = capture(current.__context__, depth + 1, seen)

		type_name = _bounded(type(current).__name__, _MAX_NAME_CHARS)
		message = _message(current)
		return RemoteFailure(
			type_name=type_name,
			type_module=_bounded(type(current).__module__, _MAX_NAME_CHARS),
			message=message,
			frames=_frames(current.__traceback__),
			traceback=_formatted_traceback(current, type_name, message),
			cause=cause,
			context=context,
		)

	return capture(exc, 0, set())


class Result(Struct, frozen=True):
	status: Literal["ok", "error", "cancelled"]
	value: bytes | None = None
	error: str | None = None
	type: str | None = None
	failure: RemoteFailure | None = None
