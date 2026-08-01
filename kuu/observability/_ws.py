from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import anyio
from anyio import create_task_group, sleep
from anyio.abc import TaskStatus

if TYPE_CHECKING:
	from starlette.types import ASGIApp
else:
	ASGIApp = Any

from kuu.observability._codec import (
	command_from_bytes,
	command_response_to_bytes,
	envelope_to_bytes,
)
from kuu.observability._commands import Cmd, CmdResponse
from kuu.marshal import marshal as _marshal
from kuu.observability._protocol import Envelope, Event, Hello, LogBatch, LogRecord

log = logging.getLogger("kuu.observability.uplink")

_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX = 30.0


class WsUplink:
	def __init__(
		self,
		url: str | None = None,
		asgi_app: ASGIApp | None = None,
		path: str = "/_ingest",
		max_buffer: int = 1000,
		token: str | None = None,
		command_handler: Callable[[Cmd], Awaitable[CmdResponse]] | None = None,
	):
		if url and asgi_app:
			raise ValueError("Cannot provide both url and asgi_app")
		if not url and not asgi_app:
			raise ValueError("Must provide either url or asgi_app")
		self._send, self._recv = anyio.create_memory_object_stream[bytes](max_buffer)
		self._url = url
		self._asgi_app = asgi_app
		self._path = path
		self._token = token
		self._last_hello: bytes | None = None
		self._command_handler = command_handler
		self._pending_gaps: dict[tuple[str, int], int] = {}
		self._gap_context: tuple[int, str, Any] | None = None

	def _auth_headers(self) -> list[tuple[str, str]]:
		return [("Authorization", f"Bearer {self._token}")] if self._token else []

	def set_command_handler(self, handler: Callable[[Cmd], Awaitable[CmdResponse]]) -> None:
		self._command_handler = handler

	async def run(self, stop_event: anyio.Event, *, task_status: TaskStatus | None = None) -> None:
		async with create_task_group() as tg:
			if self._asgi_app:
				tg.start_soon(self._run_asgi, stop_event)
			else:
				tg.start_soon(self._run_ws, stop_event)

			if task_status:
				task_status.started()

			while not stop_event.is_set():
				await sleep(0.5)

			tg.cancel_scope.cancel()

	async def _run_asgi(self, stop_event: anyio.Event) -> None:
		assert self._asgi_app
		import async_asgi_testclient

		backoff = _BACKOFF_INITIAL
		extra_headers = dict(self._auth_headers())
		while not stop_event.is_set():
			async with async_asgi_testclient.TestClient(self._asgi_app) as client:
				try:
					async with client.websocket_connect(
						path=self._path, headers=extra_headers
					) as ws:
						backoff = _BACKOFF_INITIAL
						if self._last_hello is not None:
							await ws.send_bytes(self._last_hello)
						async with create_task_group() as tg:
							tg.start_soon(self._send_asgi, ws, stop_event)
							tg.start_soon(self._receive_asgi, ws)
				except Exception as exc:
					log.warning("event=uplink.disconnected error=%s retry_in=%.1fs", exc, backoff)
			with anyio.move_on_after(backoff):
				await stop_event.wait()
			backoff = min(backoff * 2, _BACKOFF_MAX)

	async def _send_asgi(self, ws: Any, stop_event: anyio.Event) -> None:
		async for frame in self._recv:
			self._flush_gap()
			if stop_event.is_set():
				return
			await ws.send_bytes(frame)

	async def _receive_asgi(self, ws: Any) -> None:
		while True:
			await self._handle_command(await ws.receive_bytes())

	@property
	def sink(self) -> "_WsUplinkSink":
		return _WsUplinkSink(self)

	def _enqueue(self, env: Envelope) -> None:
		try:
			data = envelope_to_bytes(env)
		except Exception as e:
			log.exception("event=uplink.encode_failed error=%s", e)
			return
		if isinstance(env.body, Hello):
			self._last_hello = data
		try:
			self._send.send_nowait(data)
		except anyio.WouldBlock:
			self._record_gap(env)
		except Exception as e:
			log.exception("event=uplink.enqueue_failed error=%s", e)

	def _record_gap(self, env: Envelope) -> None:
		self._gap_context = (env.v, env.instance, env.ts)
		body = env.body
		if isinstance(body, Event) and body.message_id is not None:
			key = (body.message_id, body.attempt or 0)
			self._pending_gaps[key] = self._pending_gaps.get(key, 0) + 1
		elif isinstance(body, LogBatch):
			for record in body.records:
				key = (record.message_id, record.attempt)
				self._pending_gaps[key] = self._pending_gaps.get(key, 0) + 1

	def _flush_gap(self) -> None:
		if not self._pending_gaps or self._gap_context is None:
			return
		version, instance, ts = self._gap_context
		gap = Envelope(
			v=version,
			instance=instance,
			ts=ts,
			body=LogBatch(
				records=[
					LogRecord(
						message_id=message_id,
						attempt=attempt,
						level=30,
						logger="kuu.uplink",
						message="uplink backpressure",
						ts=time.time(),
						kind="gap",
						dropped=dropped,
					)
					for (message_id, attempt), dropped in self._pending_gaps.items()
				]
			),
		)
		try:
			self._send.send_nowait(envelope_to_bytes(gap))
			self._pending_gaps.clear()
			self._gap_context = None
		except anyio.WouldBlock:
			pass
		except Exception as exc:
			log.warning("event=uplink.gap_encode_failed error=%s", exc)

	async def _run_ws(self, stop_event: anyio.Event) -> None:
		"""maintain the ws connection until ``stop_event`` fires"""
		assert self._url

		try:
			from websockets.asyncio.client import connect as ws_connect
		except ImportError:
			log.error("event=uplink.no_websockets")
			return

		backoff = _BACKOFF_INITIAL
		headers = self._auth_headers()
		while not stop_event.is_set():
			try:
				log.info("event=uplink.connecting url=%s", self._url)
				async with ws_connect(
					self._url,
					max_size=2**20,
					additional_headers=headers,
				) as ws:
					backoff = _BACKOFF_INITIAL
					if self._last_hello is not None:
						await ws.send(self._last_hello)
					async with create_task_group() as tg:
						tg.start_soon(self._send_ws, ws, stop_event)
						tg.start_soon(self._receive_ws, ws)
			except Exception as exc:
				log.warning("event=uplink.disconnected error=%s retry_in=%.1fs", exc, backoff)
			with anyio.move_on_after(backoff):
				await stop_event.wait()
			backoff = min(backoff * 2, _BACKOFF_MAX)

	async def _send_ws(self, ws: Any, stop_event: anyio.Event) -> None:
		async for frame in self._recv:
			self._flush_gap()
			if stop_event.is_set():
				return
			await ws.send(frame)

	async def _receive_ws(self, ws: Any) -> None:
		while True:
			await self._handle_command(await ws.recv())

	async def _handle_command(self, frame: bytes | str) -> None:
		if self._command_handler is None:
			return
		try:
			command = command_from_bytes(frame)
			response = await self._command_handler(command)
		except Exception as exc:
			log.warning("event=uplink.command_failed error=%s", exc)
			return
		await self._send.send(command_response_to_bytes(response))


class _BrowserClient:
	def __init__(self, max_buffer: int) -> None:
		self.send, self.receive = anyio.create_memory_object_stream[bytes](max_buffer)
		self.subscriptions = {"runs"}
		self.gap_from: int | None = None

	def offer(self, cursor: int, payload: bytes) -> None:
		if self.gap_from is not None:
			try:
				self.send.send_nowait(
					_marshal.json_encode({"type": "gap", "from": self.gap_from, "to": cursor - 1})
				)
			except anyio.WouldBlock:
				return
			self.gap_from = None
		try:
			self.send.send_nowait(payload)
		except anyio.WouldBlock:
			self.gap_from = cursor


class BrowserStream:
	"""Bounded non-blocking fan-out for browser Run subscriptions."""

	def __init__(self, max_client_buffer: int = 256) -> None:
		self._max_client_buffer = max_client_buffer
		self._clients: set[_BrowserClient] = set()
		self._cursor = 0

	@property
	def cursor(self) -> int:
		return self._cursor

	def publish(self, envelope: Envelope) -> None:
		self._cursor += 1
		cursor = self._cursor
		for client in tuple(self._clients):
			filtered = self._filter(envelope, client.subscriptions)
			if filtered is None:
				continue
			client.offer(
				cursor,
				_marshal.json_encode({"type": "envelope", "cursor": cursor, "envelope": filtered}),
			)

	def _filter(self, envelope: Envelope, subscriptions: set[str]) -> Envelope | None:
		body = envelope.body
		if isinstance(body, LogBatch):
			records = [
				record for record in body.records if f"run:{record.message_id}" in subscriptions
			]
			if not records:
				return None
			from msgspec.structs import replace

			return replace(envelope, body=LogBatch(records=records))
		if isinstance(body, Event):
			detail = body.message_id is not None and f"run:{body.message_id}" in subscriptions
			if not detail and "runs" not in subscriptions:
				return None
			if detail:
				return envelope
			from msgspec.structs import replace

			return replace(
				envelope,
				body=replace(
					body,
					args=None,
					kwargs=None,
					headers=None,
					result_preview=None,
					exc_message=None,
					traceback=None,
					failure=None,
				),
			)
		return envelope if "runs" in subscriptions else None

	async def connect(self, websocket: Any) -> None:
		client = _BrowserClient(self._max_client_buffer)
		self._clients.add(client)
		await websocket.accept()
		await websocket.send_json({"type": "ready", "cursor": self._cursor})

		async def send() -> None:
			async with client.receive:
				async for payload in client.receive:
					await websocket.send_bytes(payload)

		async def receive() -> None:
			while True:
				data = await websocket.receive_json()
				if data.get("type") != "subscribe":
					continue
				topics = data.get("topics", [])
				if isinstance(topics, list):
					client.subscriptions = {
						str(topic)
						for topic in topics
						if topic == "runs" or str(topic).startswith("run:")
					}
				after = data.get("after")
				if isinstance(after, int) and after < self._cursor:
					client.gap_from = after + 1

		try:
			async with create_task_group() as tg:
				tg.start_soon(send)
				tg.start_soon(receive)
		except Exception:
			pass
		finally:
			self._clients.discard(client)
			await client.send.aclose()


class _WsUplinkSink:
	"""``EventsSink`` impl that delegates to ``WsUplink._enqueue``"""

	__slots__ = ("_uplink",)

	def __init__(self, uplink: WsUplink) -> None:
		self._uplink = uplink

	def emit(self, envelope: Envelope) -> None:
		self._uplink._enqueue(envelope)


__all__ = ["WsUplink", "BrowserStream"]
