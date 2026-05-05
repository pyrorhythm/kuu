from typing import Unpack

from nats.aio.client import Client
from nats.js import JetStreamContext

from kuu import NotConnected
from ._opts import NatsConnectOptions


class NatsTransport:
	_client: Client

	def __init__(self,
	             servers: str | list[str],
	             **opts: Unpack[NatsConnectOptions]) -> None:
		self._client = Client()
		self._servers = servers
		self._opts = opts

	async def connect(self) -> None:
		if self._client.is_connected:
			return
		
		await self._client.connect(servers=self._servers, **self._opts)

	async def close(self) -> None:
		if not self._client.is_connected:
			return

		await self._client.drain()

	@property
	def js(self) -> JetStreamContext:
		if not self._client.is_connected:
			raise NotConnected
		return self._client.jetstream()

	@property
	def is_connected(self) -> bool:
		return self._client.is_connected
