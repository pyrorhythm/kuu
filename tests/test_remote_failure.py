from __future__ import annotations

import builtins
from types import SimpleNamespace

import anyio
import pytest
from msgspec import json

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Settings
from kuu.exceptions import TaskError
from kuu.handle import TaskHandle
from kuu.message import Message
from kuu.result import Result
from kuu.serializers import JSONSerializer
from kuu.worker import Worker


class RoundTripResults:
	def __init__(self) -> None:
		self.serializer = JSONSerializer()
		self.marshal_types = False
		self.ttl = None
		self.replay = False
		self.store_errors = True
		self.store: dict[str, Result] = {}

	def encode(self, value):
		raise AssertionError("failure test must not encode a successful value")

	def decode(self, result):
		raise AssertionError("failure test must not decode a successful value")

	async def connect(self) -> None: ...
	async def close(self) -> None: ...

	async def get(self, key: str, **kwargs) -> Result | None:
		return self.store.get(key)

	async def set(self, key: str, result: Result, ttl=None) -> None:
		self.store[key] = self.serializer.unmarshal(self.serializer.marshal(result), into=Result)


@pytest.mark.anyio
async def test_terminal_failure_round_trips_cause_frames_source_without_locals():
	results = RoundTripResults()
	app = Kuu(broker=MemoryBroker(), results=results)

	@app.task(max_attempts=1)
	async def fail() -> None:
		try:
			try:
				runtime_secret = "-".join(("runtime", "secret", "value"))  # noqa: FLY002
				if not runtime_secret:
					raise AssertionError
				raise ValueError("root cause")
			except ValueError:
				raise LookupError("middle context")
		except LookupError as cause:
			raise RuntimeError("terminal failure") from cause

	handle = await fail.q()
	config = Settings(app="test:app", queues=["default"], concurrency=1)

	async def stop_when_stored(scope: anyio.CancelScope) -> None:
		while handle.key not in results.store:
			await anyio.sleep(0.01)
		scope.cancel()

	with anyio.fail_after(3):
		async with anyio.create_task_group() as group:
			group.start_soon(stop_when_stored, group.cancel_scope)
			group.start_soon(Worker(config, app=app).run)

	with pytest.raises(TaskError, match="RuntimeError: terminal failure") as raised:
		await handle.result(timeout=0.1)

	failure = raised.value.remote_failure
	assert failure is not None
	assert failure.type_name == "RuntimeError"
	assert failure.type_module == "builtins"
	assert failure.cause is not None
	assert failure.cause.type_name == "LookupError"
	assert failure.cause.context is not None
	assert failure.cause.context.type_name == "ValueError"
	assert "Traceback (most recent call last)" in failure.traceback
	assert "root cause" in failure.traceback

	source = "\n".join(
		line
		for item in (failure, failure.cause, failure.cause.context)
		for frame in item.frames
		for line in frame.source
	)
	assert "raise RuntimeError" in source
	assert "raise ValueError" in source
	assert "runtime-secret-value" not in json.encode(failure).decode()


@pytest.mark.anyio
async def test_arbitrary_remote_type_strings_stay_inert(monkeypatch):
	raw = b"""{"status":"error","error":"Exploit: nope","failure":{"type_name":"Exploit","type_module":"would_execute_on_import","message":"nope","frames":[],"traceback":"raw"}}"""

	def reject_import(*_args, **_kwargs):
		raise AssertionError("remote failure decoding attempted an import")

	with monkeypatch.context() as patch:
		patch.setattr(builtins, "__import__", reject_import)
		result = json.decode(raw, type=Result)

	results = RoundTripResults()
	results.store["task"] = result
	handle = TaskHandle(
		Message(task="task", queue="default", headers={"idempotency_key": "task"}),
		SimpleNamespace(results=results),
	)
	with pytest.raises(TaskError) as raised:
		await handle.result(timeout=0.1)
	assert raised.value.remote_failure is not None
	assert raised.value.remote_failure.type_module == "would_execute_on_import"


@pytest.mark.anyio
async def test_old_error_only_result_still_raises_useful_task_error():
	result = json.decode(b'{"status":"error","error":"ValueError: old failure"}', type=Result)
	assert result.failure is None

	results = RoundTripResults()
	results.store["task"] = result
	handle = TaskHandle(
		Message(task="task", queue="default", headers={"idempotency_key": "task"}),
		SimpleNamespace(results=results),
	)
	with pytest.raises(TaskError, match="ValueError: old failure") as raised:
		await handle.result(timeout=0.1)
	assert raised.value.remote_failure is None
