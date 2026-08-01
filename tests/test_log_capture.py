from __future__ import annotations

import io
import logging
import queue

import pytest

from kuu import progress
from kuu.message import Message
from kuu.observability import _log_capture


@pytest.fixture
def captured() -> queue.Queue:
	q: queue.Queue = queue.Queue()
	_log_capture.install(q, interval=60)
	yield q
	_log_capture.shutdown()


def _records(q: queue.Queue):
	_log_capture.flush()
	records = []
	while not q.empty():
		records.extend(q.get_nowait().records)
	return records


def test_captures_logging_stdout_and_progress(captured: queue.Queue) -> None:
	msg = Message(task="work", queue="q")
	token = _log_capture.set_current_msg(msg)
	try:
		logging.getLogger("task").warning("hello", extra={"order_id": 42})
		_log_capture._TaskStream(io.StringIO(), "stdout", logging.INFO).write("printed\n")
		progress(2, 5, "working", phase="parse")
		_log_capture.finish_attempt(msg)
	finally:
		_log_capture.reset_current_msg(token)

	records = _records(captured)
	assert {record.kind for record in records} >= {"log", "stdout", "progress"}
	log_record = next(record for record in records if record.kind == "log")
	assert log_record.fields == {"order_id": 42}
	assert any(record.message == "printed" for record in records)
	final = [record for record in records if record.kind == "progress"][-1]
	assert (final.current, final.total, final.fields["final"]) == (2, 5, True)
	assert [record.seq for record in records] == sorted(record.seq for record in records)


def test_final_progress_message_is_bounded(captured: queue.Queue) -> None:
	msg = Message(task="work", queue="q")
	token = _log_capture.set_current_msg(msg)
	try:
		progress(1, 1, "x" * 100_000)
		_log_capture.finish_attempt(msg)
	finally:
		_log_capture.reset_current_msg(token)

	progress_records = [record for record in _records(captured) if record.kind == "progress"]
	assert len(progress_records) == 2
	assert all(len(record.message) <= 4096 for record in progress_records)


def test_adds_optional_trace_correlation(
	captured: queue.Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
	monkeypatch.setattr(
		_log_capture,
		"_trace_fields",
		lambda: {"trace_id": "a" * 32, "span_id": "b" * 16},
	)
	msg = Message(task="work", queue="q")
	token = _log_capture.set_current_msg(msg)
	try:
		logging.getLogger("task").info("correlated")
	finally:
		_log_capture.reset_current_msg(token)

	record = _records(captured)[0]
	assert record.fields["trace_id"] == "a" * 32
	assert record.fields["span_id"] == "b" * 16


def test_attempt_budget_emits_exact_gap() -> None:
	q: queue.Queue = queue.Queue()
	_log_capture.install(q, interval=60, max_attempt_bytes=1)
	msg = Message(task="work", queue="q")
	token = _log_capture.set_current_msg(msg)
	try:
		logging.getLogger("task").warning("first")
		logging.getLogger("task").warning("second")
		_log_capture.finish_attempt(msg)
	finally:
		_log_capture.reset_current_msg(token)
		_log_capture.shutdown()

	records = []
	while not q.empty():
		records.extend(q.get_nowait().records)
	assert len(records) == 1
	assert records[0].kind == "gap"
	assert records[0].dropped == 2
