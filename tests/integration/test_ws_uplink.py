from __future__ import annotations

import socket
import time
from logging import getLogger
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
import uvicorn
from async_asgi_testclient import TestClient
from msgspec.json import encode as _json_encode

from kuu._types import _FnAsync
from kuu._util import utcnow
from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.observability import (
	PROTOCOL_VERSION,
	BrokerInfo,
	CancelCmd,
	CmdResponse,
	Envelope,
	Event,
	Hello,
	InMemoryRegistry,
	LogBatch,
	LogRecord,
	WsUplink,
	envelope_from_bytes,
	envelope_to_bytes,
)
from kuu.web.dashboard import Dashboard

pytestmark = pytest.mark.anyio

log = getLogger("kuu.test.ws")


def _free_port() -> int:
	s = socket.socket()
	s.bind(("127.0.0.1", 0))
	port = s.getsockname()[1]
	s.close()
	return port


def _hello() -> Hello:
	return Hello(
		preset="dev",
		host="h",
		pid=1,
		version="0.1.0",
		started_at=utcnow(),
		broker=BrokerInfo(type="MemoryBroker", key="kkk"),
		scheduler_enabled=False,
		processes=1,
	)


@pytest.fixture
def fresh_app() -> Kuu:
	return Kuu(broker=MemoryBroker())


@pytest.fixture
async def wsapp(
	make_app: _FnAsync[[], Kuu],
) -> AsyncIterator[tuple[WsUplink, anyio.Event, Dashboard]]:
	dash = Dashboard(app=await make_app(), registry=InMemoryRegistry())
	app = dash.build_app()
	uplink = WsUplink(asgi_app=app)
	stop = anyio.Event()
	async with anyio.create_task_group() as tg:
		tg.start_soon(uplink.run, stop)
		yield uplink, stop, dash
		stop.set()


@pytest.fixture
async def wsapp_url(make_app: _FnAsync[[], Kuu]) -> AsyncIterator[tuple[str, Dashboard]]:
	dash = Dashboard(app=await make_app(), registry=InMemoryRegistry())
	app = dash.build_app()
	port = _free_port()
	ws_url = f"ws://127.0.0.1:{port}/"
	config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
	server = uvicorn.Server(config)
	async with anyio.create_task_group() as tg:
		tg.start_soon(server.serve, None)
		await anyio.sleep(0.1)
		yield ws_url, dash
		server.should_exit = True
		await server.shutdown()


async def _drain_until(predicate, *, timeout: float = 1.0) -> None:
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		if predicate():
			return
		await anyio.sleep(0.05)
	raise AssertionError("predicate never became true")


class TestWsUplink:
	async def test_hello_then_event_lands_in_registry_and_stats(
		self, wsapp: tuple[WsUplink, anyio.Event, Dashboard]
	) -> None:
		uplink, stop, dash = wsapp

		hello = Envelope(v=PROTOCOL_VERSION, instance="abc", ts=utcnow(), body=_hello())
		ev = Envelope(
			v=PROTOCOL_VERSION,
			instance="abc",
			ts=utcnow(),
			body=Event(kind="succeeded", task="t1", queue="q", worker_pid=42, elapsed=0.1),
		)

		def _pred() -> bool:
			print(f"registry={dash.registry.all()}, stats={dash.stats.totals}")
			return bool(dash.registry.all()) and dash.stats.totals.get("succeeded", 0) >= 1

		uplink.sink.emit(hello)
		uplink.sink.emit(ev)
		await _drain_until(_pred)
		stop.set()

		roster = dash.registry.all()
		assert len(roster) == 1
		assert roster[0].hello.preset == "dev"
		assert roster[0].instance_id == "abc"
		assert dash.stats.totals["succeeded"] == 1

	async def test_re_hello_after_reconnect_preserves_instance(
		self, wsapp: tuple[WsUplink, anyio.Event, Dashboard]
	) -> None:
		uplink, stop, dash = wsapp

		hello = Envelope(v=PROTOCOL_VERSION, instance="same", ts=utcnow(), body=_hello())
		uplink.sink.emit(hello)
		await _drain_until(lambda: bool(dash.registry.all()))

		stop.set()
		app = dash.build_app()
		uplink_b = WsUplink(asgi_app=app)
		stop_b = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink_b.run, stop_b)
			uplink_b.sink.emit(hello)
			ev = Envelope(
				v=PROTOCOL_VERSION,
				instance="same",
				ts=utcnow(),
				body=Event(kind="failed", task="x", queue="q", worker_pid=1),
			)
			uplink_b.sink.emit(ev)
			await _drain_until(lambda: dash.stats.totals.get("failed", 0) >= 1)
			stop_b.set()

		roster = dash.registry.all()
		assert len(roster) == 1
		assert roster[0].instance_id == "same"

	async def test_dashboard_routes_command_back_to_preset(
		self, make_app: _FnAsync[[], Kuu]
	) -> None:
		received = []

		async def handle(command):
			received.append(command)
			return CmdResponse(
				request_id=command.request_id,
				ok=True,
				run_id=command.run_id,
			)

		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry())
		uplink = WsUplink(asgi_app=dash.build_app(), command_handler=handle)
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(
				Envelope(v=PROTOCOL_VERSION, instance="leaf-1", ts=utcnow(), body=_hello())
			)
			await _drain_until(lambda: bool(dash.registry.all()))
			response = await dash.send_command(
				"dev", CancelCmd(request_id="cancel-1", run_id="run-7")
			)
			stop.set()

		assert response.ok
		assert response.run_id == "run-7"
		assert received == [CancelCmd(request_id="cancel-1", run_id="run-7")]

	async def test_backpressure_emits_exact_observation_gap(
		self, make_app: _FnAsync[[], Kuu]
	) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry())
		uplink = WsUplink(asgi_app=dash.build_app(), max_buffer=1)
		uplink.sink.emit(
			Envelope(
				v=PROTOCOL_VERSION,
				instance="leaf-1",
				ts=utcnow(),
				body=Event(
					kind="started",
					task="work",
					queue="q",
					worker_pid=1,
					message_id="run-1",
					attempt=0,
				),
			)
		)
		uplink.sink.emit(
			Envelope(
				v=PROTOCOL_VERSION,
				instance="leaf-1",
				ts=utcnow(),
				body=LogBatch(
					records=[
						LogRecord(
							message_id="run-1",
							attempt=0,
							level=20,
							logger="task",
							message=message,
							ts=0,
						)
						for message in ("lost-1", "lost-2")
					]
				),
			)
		)
		uplink._recv.receive_nowait()  # sender frees the single buffered slot
		uplink._flush_gap()
		gap = envelope_from_bytes(uplink._recv.receive_nowait())
		assert isinstance(gap.body, LogBatch)
		assert gap.body.records[0].kind == "gap"
		assert gap.body.records[0].dropped == 2
		assert uplink._pending_gaps == {}

		uplink.sink.emit(
			Envelope(
				v=PROTOCOL_VERSION,
				instance="leaf-1",
				ts=utcnow(),
				body=Event(
					kind="succeeded",
					task="work",
					queue="q",
					worker_pid=1,
					message_id="run-1",
					attempt=0,
				),
			)
		)
		current = envelope_from_bytes(uplink._recv.receive_nowait())
		assert isinstance(current.body, Event)
		assert current.body.kind == "succeeded"

	async def test_disconnected_uplink_marks_attempts_unknown_after_timeout(
		self, make_app: _FnAsync[[], Kuu], monkeypatch: pytest.MonkeyPatch
	) -> None:
		import kuu.web.dashboard as dashboard_module

		monkeypatch.setattr(dashboard_module, "UPLINK_LOST_AFTER", 0.2)
		worker = MagicMock()
		worker.backend.mark_instance_unknown = AsyncMock(return_value=1)
		dashboard = Dashboard(
			app=await make_app(),
			registry=InMemoryRegistry(),
			persistence_worker=worker,
		)
		async with TestClient(dashboard.build_app()) as client:
			async with client.websocket_connect(path="/_ingest") as ws:
				await ws.send_bytes(
					envelope_to_bytes(
						Envelope(
							v=PROTOCOL_VERSION,
							instance="lost-leaf",
							ts=utcnow(),
							body=_hello(),
						)
					)
				)

		await _drain_until(lambda: worker.backend.mark_instance_unknown.await_count == 1)
		assert worker.backend.mark_instance_unknown.await_args.args[0] == "lost-leaf"

	async def test_idle_uplink_is_lost_after_heartbeat_timeout(
		self, make_app: _FnAsync[[], Kuu], monkeypatch: pytest.MonkeyPatch
	) -> None:
		import kuu.web.dashboard as dashboard_module

		monkeypatch.setattr(dashboard_module, "UPLINK_LOST_AFTER", 0.02)
		worker = MagicMock()
		worker.backend.mark_instance_unknown = AsyncMock(return_value=1)
		dashboard = Dashboard(
			app=await make_app(),
			registry=InMemoryRegistry(),
			persistence_worker=worker,
		)
		async with (
			TestClient(dashboard.build_app()) as client,
			client.websocket_connect(path="/_ingest") as ws,
		):
			await ws.send_bytes(
				envelope_to_bytes(
					Envelope(
						v=PROTOCOL_VERSION,
						instance="idle-leaf",
						ts=utcnow(),
						body=_hello(),
					)
				)
			)
			await _drain_until(lambda: worker.backend.mark_instance_unknown.await_count == 1)

		assert worker.backend.mark_instance_unknown.await_args.args[0] == "idle-leaf"

	async def test_unknown_tag_is_dropped_not_fatal(
		self, wsapp: tuple[WsUplink, anyio.Event, Dashboard]
	) -> None:
		uplink, _, dash = wsapp
		app = uplink._asgi_app

		async with TestClient(app) as tc, tc.websocket_connect(path="/_ingest") as ws:
			await ws.send_bytes(
				envelope_to_bytes(
					Envelope(v=PROTOCOL_VERSION, instance="zzz", ts=utcnow(), body=_hello())
				)
			)
			await ws.send_bytes(
				_json_encode(
					{
						"v": PROTOCOL_VERSION,
						"instance": "zzz",
						"ts": utcnow(),
						"body": {"type": "totally-bogus"},
					}
				)
			)
			await ws.send_bytes(
				envelope_to_bytes(
					Envelope(
						v=PROTOCOL_VERSION,
						instance="zzz",
						ts=utcnow(),
						body=Event(kind="failed", task="work", queue="q", worker_pid=1),
					)
				)
			)
			await _drain_until(lambda: dash.stats.totals.get("failed") == 1)

		assert any(entry.instance_id == "zzz" for entry in dash.registry.all())


class TestBrowserStream:
	async def test_collector_forwards_events_and_logs_to_persistence(
		self, make_app: _FnAsync[[], Kuu]
	) -> None:
		worker = MagicMock()
		dashboard = Dashboard(
			app=await make_app(),
			registry=InMemoryRegistry(),
			persistence_worker=worker,
		)
		event = Event(
			kind="started",
			task="work",
			queue="q",
			worker_pid=1,
			message_id="run-1",
		)
		batch = LogBatch(
			records=[
				LogRecord(
					message_id="run-1",
					attempt=0,
					level=20,
					logger="task",
					message="hello",
					ts=0,
				)
			]
		)
		dashboard.ingest_envelope(
			Envelope(v=PROTOCOL_VERSION, instance="leaf", ts=utcnow(), body=event)
		)
		dashboard.ingest_envelope(
			Envelope(v=PROTOCOL_VERSION, instance="leaf", ts=utcnow(), body=batch)
		)

		worker.enqueue_event.assert_called_once_with("leaf", event)
		worker.enqueue_log_batch.assert_called_once_with("leaf", batch)

	async def test_compact_runs_and_selected_run_logs(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry())
		async with (
			TestClient(dash.build_app()) as client,
			client.websocket_connect(path="/ws") as ws,
		):
			assert (await ws.receive_json())["type"] == "ready"
			dash.ingest_envelope(
				Envelope(
					v=PROTOCOL_VERSION,
					instance="preset-1",
					ts=utcnow(),
					body=Event(
						kind="started",
						task="work",
						queue="q",
						worker_pid=1,
						message_id="run-1",
						args=["private"],
					),
				)
			)
			compact = await ws.receive_json()
			assert compact["envelope"]["body"]["args"] is None

			await ws.send_json({"type": "subscribe", "topics": ["runs", "run:run-1"]})
			await anyio.sleep(0)
			dash.ingest_envelope(
				Envelope(
					v=PROTOCOL_VERSION,
					instance="preset-1",
					ts=utcnow(),
					body=LogBatch(
						records=[
							LogRecord(
								message_id="run-1",
								attempt=0,
								level=20,
								logger="task",
								message="visible",
								ts=0,
							),
							LogRecord(
								message_id="other",
								attempt=0,
								level=20,
								logger="task",
								message="hidden",
								ts=0,
							),
						],
					),
				)
			)
			detail = await ws.receive_json()
			assert [r["message"] for r in detail["envelope"]["body"]["records"]] == ["visible"]


class TestWsUplinkAuth:
	async def test_authorized_uplink_passes(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry(), ingest_token="s3cret")
		asgi = dash.build_app()

		uplink = WsUplink(asgi_app=asgi, token="s3cret")
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(
				Envelope(v=PROTOCOL_VERSION, instance="ok", ts=utcnow(), body=_hello())
			)
			await _drain_until(lambda: any(e.instance_id == "ok" for e in dash.registry.all()))
			stop.set()

	async def test_missing_token_is_rejected(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry(), ingest_token="s3cret")
		asgi = dash.build_app()
		uplink = WsUplink(asgi_app=asgi)
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(
				Envelope(v=PROTOCOL_VERSION, instance="nope", ts=utcnow(), body=_hello())
			)
			await anyio.sleep(0.5)
			stop.set()

		assert not any(e.instance_id == "nope" for e in dash.registry.all())

	async def test_wrong_token_is_rejected(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry(), ingest_token="s3cret")
		asgi = dash.build_app()

		uplink = WsUplink(asgi_app=asgi, token="wrong")
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(
				Envelope(v=PROTOCOL_VERSION, instance="bad", ts=utcnow(), body=_hello())
			)
			await anyio.sleep(0.5)
			stop.set()

		assert not any(e.instance_id == "bad" for e in dash.registry.all())
