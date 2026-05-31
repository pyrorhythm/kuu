from __future__ import annotations

import logging
import os
import typing

from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs._internal.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

if typing.TYPE_CHECKING:
	from opentelemetry.sdk._logs._internal.export import LogRecordExporter

log = logging.getLogger("kuu.otel")


class OtelLoggingBridge:
	"""Bridges Python's ``logging`` to an OpenTelemetry ``LoggerProvider``.

	Attaches an OTel ``LoggingHandler`` to the named stdlib logger so that
	any log records emitted through it are forwarded to the provider.  Does
	**not** configure any particular logging frontend : use ``logging``,
	``structlog``, or anything else that ultimately produces
	``logging.LogRecord`` instances.

	Provider resolution order (first wins):
	  1. ``logger_provider`` passed to :meth:`setup`.
	  2. ``log_exporter`` passed to :meth:`setup` : a new ``LoggerProvider``
	     is created around it.
	  3. Falls back to OTLP HTTP (``OTLPLogExporter``) only when
	     ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set and neither of the above
	     was supplied.
	"""

	def __init__(
		self,
		*,
		level: int = logging.INFO,
		logger_name: str = "kuu",
	):
		self._level = level
		self._logger_name = logger_name
		self._provider: LoggerProvider | None = None
		self._handler: LoggingHandler | None = None
		self._owns_provider: bool = False

	def setup(
		self,
		resource: Resource | None = None,
		*,
		logger_provider: LoggerProvider | None = None,
		log_exporter: LogRecordExporter | None = None,
	) -> logging.Logger:
		if logger_provider is not None:
			self._provider = logger_provider
			self._owns_provider = False
		else:
			exporter = log_exporter
			if exporter is None and os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
				from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

				exporter = OTLPLogExporter()

			if exporter is not None:
				res = resource or Resource.create({})
				self._provider = LoggerProvider(resource=res)
				self._provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
				self._owns_provider = True

		if self._provider is None:
			log.debug("event=otel.no_provider")
			return logging.getLogger(self._logger_name)

		self._handler = LoggingHandler(level=self._level, logger_provider=self._provider)
		logger = logging.getLogger(self._logger_name)
		logger.addHandler(self._handler)
		log.info("event=otel.bridge_attached logger=%s", self._logger_name)
		return logger

	def shutdown(self) -> None:
		if self._handler is not None:
			logging.getLogger(self._logger_name).removeHandler(self._handler)
			self._handler = None
		if self._owns_provider and self._provider is not None:
			self._provider.shutdown()
		self._provider = None


__all__ = ("OtelLoggingBridge",)
