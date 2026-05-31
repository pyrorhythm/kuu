from __future__ import annotations

import logging
import os
import typing

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from ._logging import OtelLoggingBridge
from ._metrics import OtelMetrics
from ._traces import OtelTracingMiddleware

if typing.TYPE_CHECKING:
	from opentelemetry.sdk._logs._internal.export import LogRecordExporter
	from opentelemetry.sdk.metrics.export import MetricExporter
	from opentelemetry.sdk.trace.export import SpanExporter

	from kuu.app import Kuu

log = logging.getLogger("kuu.otel.instrumentor")


def _auto_setup_sdk(
	endpoint: str | None = None,
	*,
	span_exporter: SpanExporter | None = None,
	metric_exporter: MetricExporter | None = None,
) -> Resource | None:
	endpoint = endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if span_exporter is None and metric_exporter is None and not endpoint:
		log.debug("event=otel.no_endpoint")
		return None

	service_name = os.getenv("OTEL_SERVICE_NAME", "kuu")
	resource = Resource.create({"service.name": service_name})
	log.info("event=otel.auto_setup service=%s", service_name)

	if span_exporter is None:
		from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

		span_exporter = OTLPSpanExporter(endpoint=endpoint)

	tp = TracerProvider(resource=resource)
	tp.add_span_processor(BatchSpanProcessor(span_exporter))
	trace.set_tracer_provider(tp)

	if metric_exporter is None:
		from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

		metric_exporter = OTLPMetricExporter(endpoint=endpoint)

	reader = PeriodicExportingMetricReader(metric_exporter)
	mp = MeterProvider(resource=resource, metric_readers=[reader])
	metrics.set_meter_provider(mp)

	return resource


class KuuOTELInstrumentor:
	"""
	Wires OTEL instrumentation (traces + metrics + logs) into a Kuu app.

	Basic usage (uses globally configured OTel providers: bring your own
	SDK setup)::

	    KuuOTELInstrumentor(app=app).instrument(setup_sdk=False)

	Auto-setup with OTLP HTTP (reads ``OTEL_EXPORTER_OTLP_ENDPOINT``)::

	    KuuOTELInstrumentor(app=app).instrument()

	Bring your own exporters (e.g. gRPC)::

	    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
	    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
	    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

	    KuuOTELInstrumentor(
	        app=app,
	        span_exporter=OTLPSpanExporter(...),
	        metric_exporter=OTLPMetricExporter(...),
	        log_exporter=OTLPLogExporter(...),
	    ).instrument()

	``instrument()`` will:
	  1. Configure OTel SDK providers when exporters are supplied or
	     ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set (skipped with
	     ``setup_sdk=False``).
	  2. Insert ``OtelTracingMiddleware`` at the front of the middleware
	     chain.
	  3. Wire ``OtelMetrics`` onto app.events signals.
	  4. Bridge stdlib ``logging`` to the log provider when a log exporter
	     or ``OTEL_EXPORTER_OTLP_ENDPOINT`` is available.
	"""

	def __init__(
		self,
		*,
		app: Kuu,
		tracer_name: str = "kuu",
		meter_name: str = "kuu",
		propagate: bool = True,
		log_level: int = logging.INFO,
		logger_name: str = "kuu",
		span_exporter: SpanExporter | None = None,
		metric_exporter: MetricExporter | None = None,
		log_exporter: LogRecordExporter | None = None,
	):
		self._app = app
		self._tracer_name = tracer_name
		self._meter_name = meter_name
		self._propagate = propagate
		self._log_level = log_level
		self._logger_name = logger_name
		self._span_exporter: SpanExporter | None = span_exporter
		self._metric_exporter = metric_exporter
		self._log_exporter = log_exporter

		self._otel_middleware: OtelTracingMiddleware | None = None
		self._otel_metrics: OtelMetrics | None = None
		self._logging_bridge: OtelLoggingBridge | None = None

	def instrument(self, *, setup_sdk: bool = True) -> None:
		resource: Resource | None = None
		if setup_sdk:
			resource = _auto_setup_sdk(
				span_exporter=self._span_exporter,
				metric_exporter=self._metric_exporter,
			)

		self._otel_middleware = OtelTracingMiddleware(
			tracer_name=self._tracer_name,
			propagate_ctx=self._propagate,
		)
		self._app.middleware.insert(0, self._otel_middleware)

		self._otel_metrics = OtelMetrics(
			app=self._app,
			meter_name=self._meter_name,
		)

		self._logging_bridge = OtelLoggingBridge(
			level=self._log_level,
			logger_name=self._logger_name,
		)
		self._logging_bridge.setup(resource=resource, log_exporter=self._log_exporter)

	def uninstrument(self) -> None:
		if self._otel_middleware is not None:
			try:
				self._app.middleware.remove(self._otel_middleware)
			except ValueError:
				pass
			self._otel_middleware = None

		if self._otel_metrics is not None:
			self._otel_metrics.disconnect()
			self._otel_metrics = None

		if self._logging_bridge is not None:
			self._logging_bridge.shutdown()
			self._logging_bridge = None


def shutdown_telemetry() -> None:
	from contextlib import suppress

	with suppress(Exception):
		prov = trace.get_tracer_provider()
		if hasattr(prov, "shutdown") and callable(prov.shutdown):
			prov.shutdown()

	with suppress(Exception):
		prov = metrics.get_meter_provider()
		if hasattr(prov, "shutdown") and callable(prov.shutdown):
			prov.shutdown()


__all__ = ("shutdown_telemetry", "KuuOTELInstrumentor")
