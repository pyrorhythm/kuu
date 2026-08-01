from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

from kuu.message import Message
from kuu.observability import EnqueueCmd, RetryCmd
from kuu.orchestrator._command_service import CommandService

pytestmark = pytest.mark.anyio


def _service():
	def task(value: int) -> None:
		pass

	definition = SimpleNamespace(original_func=task, max_attempts=2)
	app = SimpleNamespace(
		registry=SimpleNamespace(get=MagicMock(return_value=definition)),
		broker=SimpleNamespace(connect=AsyncMock()),
		enqueue_by_name=AsyncMock(
			return_value=SimpleNamespace(message=SimpleNamespace(id="run-1"))
		),
		_build_message=MagicMock(return_value=Message(task="task", queue="default")),
		_dispatch=AsyncMock(),
	)
	loader = SimpleNamespace(get=MagicMock(return_value=app))
	return CommandService(loader), app


async def test_enqueue_is_validated_and_deduplicated_by_request_id() -> None:
	service, app = _service()
	command = EnqueueCmd(request_id="same", task="task", args=[3])

	first = await service.dispatch(command)
	second = await service.dispatch(command)

	assert first == second
	assert first.ok and first.run_id == "run-1"
	app.enqueue_by_name.assert_awaited_once()


async def test_concurrent_duplicate_waits_for_first_response() -> None:
	service, app = _service()
	gate = anyio.Event()
	result = SimpleNamespace(message=SimpleNamespace(id="run-1"))

	async def enqueue(*args, **kwargs):
		await gate.wait()
		return result

	app.enqueue_by_name.side_effect = enqueue
	responses = []
	command = EnqueueCmd(request_id="same", task="task", args=[3])

	async def dispatch() -> None:
		responses.append(await service.dispatch(command))

	async with anyio.create_task_group() as tg:
		tg.start_soon(dispatch)
		while app.enqueue_by_name.await_count == 0:
			await anyio.sleep(0)
		tg.start_soon(dispatch)
		await anyio.sleep(0)
		gate.set()

	assert responses[0] == responses[1]
	app.enqueue_by_name.assert_awaited_once()


async def test_retry_preserves_run_id_and_creates_requested_attempt() -> None:
	service, app = _service()
	run_id = "1d42655e-70f5-47bd-a83d-e3efc8b09ebf"

	response = await service.dispatch(
		RetryCmd(
			request_id="retry",
			run_id=run_id,
			attempt=3,
			task="task",
			args=[7],
		)
	)

	assert response.ok and response.run_id == run_id
	message = app._dispatch.await_args.args[0]
	assert str(message.id) == run_id
	assert message.attempt == 3
	assert app._build_message.call_args.kwargs["headers"] == {"kuu.operator_retry": "true"}


async def test_invalid_arguments_are_rejected_before_enqueue() -> None:
	service, app = _service()

	response = await service.dispatch(
		EnqueueCmd(request_id="invalid", task="task", kwargs={"value": 3, "missing": 4})
	)

	assert not response.ok
	assert "unexpected keyword" in (response.error or "")
	app.enqueue_by_name.assert_not_awaited()
