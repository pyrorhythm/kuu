from __future__ import annotations

from typing import Any

import aio_pika

from kuu.exceptions import NotConnected


class RabbitMQTransport:
	def __init__(
		self,
		url: str = "amqp://guest:guest@localhost/",
		*,
		connection: Any | None = None,
	) -> None:
		self.url = url
		self._connection = connection
		self._channel: Any | None = None

	async def connect(self) -> None:
		if self._connection is not None and not self._connection.is_closed:
			if self._channel is None or self._channel.is_closed:
				self._channel = await self._connection.channel()
			return
		self._connection = await aio_pika.connect_robust(self.url)
		self._channel = await self._connection.channel()

	async def close(self) -> None:
		if self._channel is not None and not self._channel.is_closed:
			await self._channel.close()
		if self._connection is not None and not self._connection.is_closed:
			await self._connection.close()
		self._channel = None
		self._connection = None

	@property
	def connection(self) -> Any:
		if self._connection is None or self._connection.is_closed:
			raise NotConnected("rabbitmq transport not connected")
		return self._connection

	@property
	def channel(self) -> Any:
		if self._channel is None or self._channel.is_closed:
			raise NotConnected("rabbitmq transport not connected")
		return self._channel

	@property
	def is_connected(self) -> bool:
		return self._connection is not None and not self._connection.is_closed

	def queue_key(self, prefix: str, queue: str) -> str:
		return f"{prefix}{queue}"
