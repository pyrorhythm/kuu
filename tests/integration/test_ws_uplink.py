from __future__ import annotations

import socket
import time
from logging import getLogger
from typing import AsyncIterator

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
	BrokerInfo,
	Envelope,
	Event,
	Hello,
	InMemoryRegistry,
	WsUplink,
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

		hello = Envelope(v=1, instance="abc", ts=utcnow(), body=_hello())
		ev = Envelope(
			v=1,
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

		hello = Envelope(v=1, instance="same", ts=utcnow(), body=_hello())
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
				v=1,
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

	async def test_unknown_tag_is_dropped_not_fatal(
		self, wsapp: tuple[WsUplink, anyio.Event, Dashboard]
	) -> None:
		uplink, _, dash = wsapp
		app = uplink._asgi_app

		async with TestClient(app) as tc, tc.websocket_connect(path="/_ingest") as ws:
			await ws.send_bytes(
				_json_encode(
					{"v": 1, "instance": "x", "ts": utcnow(), "body": {"type": "totally-bogus"}}
				)
			)
			await ws.send_bytes(
				envelope_to_bytes(Envelope(v=1, instance="zzz", ts=utcnow(), body=_hello()))
			)

		await _drain_until(lambda: any(e.instance_id == "zzz" for e in dash.registry.all()))


class TestWsUplinkAuth:
	async def test_authorized_uplink_passes(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry(), ingest_token="s3cret")
		asgi = dash.build_app()

		uplink = WsUplink(asgi_app=asgi, token="s3cret")
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(Envelope(v=1, instance="ok", ts=utcnow(), body=_hello()))
			await _drain_until(lambda: any(e.instance_id == "ok" for e in dash.registry.all()))
			stop.set()

	async def test_missing_token_is_rejected(self, make_app: _FnAsync[[], Kuu]) -> None:
		dash = Dashboard(app=await make_app(), registry=InMemoryRegistry(), ingest_token="s3cret")
		asgi = dash.build_app()
		uplink = WsUplink(asgi_app=asgi)
		stop = anyio.Event()
		async with anyio.create_task_group() as tg:
			tg.start_soon(uplink.run, stop)
			uplink.sink.emit(Envelope(v=1, instance="nope", ts=utcnow(), body=_hello()))
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
			uplink.sink.emit(Envelope(v=1, instance="bad", ts=utcnow(), body=_hello()))
			await anyio.sleep(0.5)
			stop.set()

		assert not any(e.instance_id == "bad" for e in dash.registry.all())
