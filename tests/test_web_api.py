from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from async_asgi_testclient import TestClient

from kuu._util import utcnow
from kuu.observability import (
	PROTOCOL_VERSION,
	BrokerInfo,
	CmdResponse,
	Envelope,
	Hello,
	InMemoryRegistry,
	RetryCmd,
	TaskInfo,
)
from kuu.persistence import LogicalRunRow, RunRow
from kuu.web.dashboard import Dashboard

pytestmark = pytest.mark.anyio

RUN_ID = "1d42655e-70f5-47bd-a83d-e3efc8b09ebf"


def _dashboard(status: str = "unknown", *, input_complete: bool = True):
	registry = InMemoryRegistry()
	registry.ingest(
		Envelope(
			v=PROTOCOL_VERSION,
			instance="leaf-1",
			ts=utcnow(),
			body=Hello(
				preset="lunar",
				host="host",
				pid=1,
				version="test",
				started_at=utcnow(),
				broker=BrokerInfo(type="memory", key="memory"),
				scheduler_enabled=False,
				processes=1,
				tasks=[TaskInfo(name="work", queue="q")],
			),
		)
	)
	logical = LogicalRunRow(
		message_id=RUN_ID,
		task="work",
		queue="q",
		instance_id="old-leaf",
		status=status,  # type: ignore[arg-type]
		created_at=utcnow(),
		updated_at=utcnow(),
	)
	attempt = RunRow(
		message_id=RUN_ID,
		attempt=1,
		task="work",
		queue="q",
		instance_id="old-leaf",
		status="started",
		args=[7],
		kwargs={},
		input_complete=input_complete,
	)
	backend = SimpleNamespace(
		get_logical_run=AsyncMock(return_value=logical),
		query_run_attempts=AsyncMock(return_value=[attempt]),
	)
	control = SimpleNamespace(
		send_command=AsyncMock(
			return_value=CmdResponse(request_id="request", ok=True, run_id=RUN_ID)
		)
	)
	return Dashboard(
		registry=registry,
		control=control,
		persistence_backend=backend,
	), control


async def test_runs_endpoint_declares_live_only_without_persistence() -> None:
	async with TestClient(Dashboard().build_app()) as client:
		response = await client.get("/api/task-runs")

	assert response.status_code == 200
	assert response.json()["live_only"] is True


async def test_retry_unknown_run_creates_next_attempt_on_preset() -> None:
	dashboard, control = _dashboard()
	async with TestClient(dashboard.build_app()) as client:
		response = await client.post("/api/retry-run", json={"run_id": RUN_ID})

	assert response.status_code == 200
	target, command = control.send_command.await_args.args
	assert target == "lunar"
	assert isinstance(command, RetryCmd)
	assert command.run_id == RUN_ID
	assert command.attempt == 2
	assert command.args == [7]


async def test_retry_and_replay_reject_incomplete_captured_input() -> None:
	retry_dashboard, retry_control = _dashboard(input_complete=False)
	replay_dashboard, replay_control = _dashboard("failed", input_complete=False)
	async with TestClient(retry_dashboard.build_app()) as client:
		retry = await client.post("/api/retry-run", json={"run_id": RUN_ID})
	async with TestClient(replay_dashboard.build_app()) as client:
		replay = await client.post("/api/replay-run", json={"run_id": RUN_ID})

	assert retry.status_code == 409
	assert replay.status_code == 409
	assert "complete input" in retry.json()["error"]
	assert "complete input" in replay.json()["error"]
	retry_control.send_command.assert_not_awaited()
	replay_control.send_command.assert_not_awaited()


async def test_replay_rejects_active_run() -> None:
	dashboard, control = _dashboard("running")
	async with TestClient(dashboard.build_app()) as client:
		response = await client.post("/api/replay-run", json={"run_id": RUN_ID})

	assert response.status_code == 409
	assert "terminal Run" in response.json()["error"]
	control.send_command.assert_not_awaited()


async def test_retry_rejects_terminal_run() -> None:
	dashboard, control = _dashboard("failed")
	async with TestClient(dashboard.build_app()) as client:
		response = await client.post("/api/retry-run", json={"run_id": RUN_ID})

	assert response.status_code == 409
	control.send_command.assert_not_awaited()
