from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import anyio
from anyio import create_task_group, sleep
from anyio.abc import TaskStatus
from starlette.types import ASGIApp

from kuu.observability._codec import envelope_to_bytes
from kuu.observability._protocol import Envelope, Hello

if TYPE_CHECKING:
	pass

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

	def _auth_headers(self) -> list[tuple[str, str]]:
		return [("Authorization", f"Bearer {self._token}")] if self._token else []

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
						if self._last_hello is not None:
							await ws.send_bytes(self._last_hello)
						async for frame in self._recv:
							log.debug("uplink received %s", frame)
							if stop_event.is_set():
								break
							await ws.send_bytes(frame)
							log.debug("uplink sent %s", frame)
				except Exception as exc:
					log.warning("uplink disconnected: %s; retrying in %.1fs", exc, backoff)
					with anyio.move_on_after(backoff):
						await stop_event.wait()
					if stop_event.is_set():
						return
					backoff = min(backoff * 2, _BACKOFF_MAX)

	@property
	def sink(self) -> "_WsUplinkSink":
		return _WsUplinkSink(self)

	def _enqueue(self, env: Envelope) -> None:
		try:
			data = envelope_to_bytes(env)
		except Exception:
			log.exception("envelope encode failed")
			return
		if isinstance(env.body, Hello):
			self._last_hello = data
		try:
			self._send.send_nowait(data)
		except anyio.WouldBlock:
			pass  # drop on backpressure
		except Exception:
			log.exception("uplink enqueue failed")

	async def _run_ws(self, stop_event: anyio.Event) -> None:
		"""maintain the ws connection until ``stop_event`` fires"""
		assert self._url

		try:
			from websockets.asyncio.client import connect as ws_connect
		except ImportError:
			log.error("websockets package not installed; uplink disabled")
			return

		backoff = _BACKOFF_INITIAL
		headers = self._auth_headers()
		while not stop_event.is_set():
			try:
				log.info("uplink connecting to %s", self._url)
				async with ws_connect(
					self._url,
					max_size=2**20,
					additional_headers=headers,
				) as ws:
					backoff = _BACKOFF_INITIAL
					if self._last_hello is not None:
						await ws.send(self._last_hello)
					async for frame in self._recv:
						log.debug("uplink received %s", frame)
						if stop_event.is_set():
							break
						await ws.send(frame)
						log.debug("uplink sent %s", frame)
				return
			except Exception as exc:
				log.warning("uplink disconnected: %s; retrying in %.1fs", exc, backoff)
				with anyio.move_on_after(backoff):
					await stop_event.wait()
				if stop_event.is_set():
					return
				backoff = min(backoff * 2, _BACKOFF_MAX)


class _WsUplinkSink:
	"""``EventsSink`` impl that delegates to ``WsUplink._enqueue``"""

	__slots__ = ("_uplink",)

	def __init__(self, uplink: WsUplink) -> None:
		self._uplink = uplink

	def emit(self, envelope: Envelope) -> None:
		self._uplink._enqueue(envelope)


__all__ = ["WsUplink"]
