from __future__ import annotations

import logging
import os
import typing
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import metrics, propagate, trace
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs._internal.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer

if typing.TYPE_CHECKING:
	from opentelemetry.sdk._logs._internal.export import LogRecordExporter
	from opentelemetry.sdk.metrics.export import MetricExporter
	from opentelemetry.sdk.trace.export import SpanExporter

	from .app import Kuu
	from .context import Context
	from .message import Message
	from .middleware.base import Next


__all__ = [
	"KuuOTELInstrumentor",
	"OtelTracingMiddleware",
	"OtelMetrics",
	"OtelLoggingBridge",
	"shutdown_telemetry",
]


log = logging.getLogger("kuu.otel")


def _span_kind_for_phase(phase: str) -> SpanKind:
	return SpanKind.PRODUCER if phase == "enqueue" else SpanKind.CONSUMER


def _span_name(ctx: Context) -> str:
	task_name = ctx.task.task_name if ctx.task else ctx.message.task
	verb = "publish" if ctx.phase == "enqueue" else "process"
	return f"{task_name} {verb}"


def _set_messaging_attributes(span: Any, ctx: Context) -> None:
	msg = ctx.message
	span.set_attribute("messaging.system", "kuu")
	span.set_attribute("messaging.operation", "publish" if ctx.phase == "enqueue" else "process")
	span.set_attribute("messaging.destination", msg.queue)
	span.set_attribute("messaging.destination.name", msg.queue)
	span.set_attribute("messaging.message.id", str(msg.id))
	span.set_attribute("kuu.task.name", msg.task)
	span.set_attribute("kuu.task.attempt", msg.attempt)
	span.set_attribute("kuu.task.max_attempts", msg.max_attempts)


class OtelTracingMiddleware:
	"""
	Middleware that creates OTEL spans around enqueue and process phases.

	Enqueue phase (PRODUCER):
		1. Start span ``{task_name} publish`` with PRODUCER kind.
		2. Set messaging attributes.
		3. Inject W3C TraceContext into ``ctx.message.headers``.
		4. Execute the middleware chain.
		5. End the span (set ERROR status + record exception on failure).

	Process phase (CONSUMER):
		1. Extract W3C TraceContext from ``ctx.message.headers``.
		2. Attach extracted context.
		3. Start span ``{task_name} process`` with CONSUMER kind.
		4. Execute the middleware chain.
		5. Set OK / ERROR status.
		6. Detach context and end span.
	"""

	def __init__(
		self,
		*,
		tracer_name: str = "kuu",
		propagate_ctx: bool = True,
	):
		self._tracer_name = tracer_name
		self._propagate = propagate_ctx
		self._tracer: Tracer | None = None

	@property
	def tracer(self) -> Tracer:
		if self._tracer is None:
			self._tracer = trace.get_tracer(self._tracer_name)
		return self._tracer

	async def __call__(self, ctx: Context, call_next: Next) -> Any:
		if ctx.phase == "enqueue":
			return await self._enqueue(ctx, call_next)
		if ctx.phase == "process":
			return await self._process(ctx, call_next)

	async def _enqueue(self, ctx: Context, call_next: Next) -> Any:
		span = self.tracer.start_span(
			_span_name(ctx),
			kind=SpanKind.PRODUCER,
		)
		_set_messaging_attributes(span, ctx)

		with trace.use_span(span, end_on_exit=True):
			if self._propagate:
				propagate.inject(ctx.message.headers)

			try:
				return await call_next()
			except Exception as exc:
				span.set_status(Status(StatusCode.ERROR))
				span.record_exception(exc)
				raise

	async def _process(self, ctx: Context, call_next: Next) -> Any:
		extracted = propagate.extract(carrier=ctx.message.headers) if self._propagate else None
		token = otel_context.attach(extracted) if extracted else None

		span = self.tracer.start_span(
			_span_name(ctx),
			kind=SpanKind.CONSUMER,
		)
		_set_messaging_attributes(span, ctx)

		with trace.use_span(span, end_on_exit=True):
			try:
				result = await call_next()
				span.set_status(Status(StatusCode.OK))
				return result
			except Exception as exc:
				span.set_status(Status(StatusCode.ERROR))
				span.record_exception(exc)
				raise
			finally:
				if token is not None:
					otel_context.detach(token)


class OtelMetrics:
	"""
	Wires OTEL metrics onto Kuu task lifecycle events.

	Instruments created (all under ``meter_name`` scope):
		- ``kuu.task.enqueued`` (Counter) : tasks enqueued
		- ``kuu.task.processed`` (Counter) : tasks processed, with ``status`` label
		- ``kuu.task.duration`` (Histogram, seconds) : task execution duration
		- ``kuu.task.in_flight`` (UpDownCounter) : tasks currently executing
		- ``kuu.task.retried`` (Counter) : retry attempts
	"""

	def __init__(
		self,
		*,
		app: Kuu,
		meter_name: str = "kuu",
	):
		self._app = app
		self._meter = metrics.get_meter(meter_name)

		self._enqueued = self._meter.create_counter(
			"kuu.task.enqueued",
			"tasks",
			"Tasks enqueued",
		)
		self._processed = self._meter.create_counter(
			"kuu.task.processed",
			"tasks",
			"Tasks processed by outcome",
		)
		self._duration = self._meter.create_histogram(
			"kuu.task.duration",
			"s",
			"Task execution duration",
		)
		self._in_flight = self._meter.create_up_down_counter(
			"kuu.task.in_flight",
			"tasks",
			"Tasks currently executing",
		)
		self._retried = self._meter.create_counter(
			"kuu.task.retried",
			"tasks",
			"Task retry attempts",
		)

		self._inflight_ids = set()

		ev = app.events
		ev.task_enqueued.connect(self._on_enqueued)
		ev.task_started.connect(self._on_started)
		ev.task_succeeded.connect(self._on_succeeded)
		ev.task_failed.connect(self._on_failed)
		ev.task_retried.connect(self._on_retried)
		ev.task_dead.connect(self._on_dead)

	def _attrs(self, msg: Message, **extra: str) -> dict[str, Any]:
		base: dict[str, Any] = {"task.name": msg.task, "queue": msg.queue}
		base.update(extra)
		return base

	def _settle(self, msg: Message) -> None:
		if msg.id in self._inflight_ids:
			self._inflight_ids.discard(msg.id)
			self._in_flight.add(-1, self._attrs(msg))

	def _on_enqueued(self, msg: Message) -> None:
		self._enqueued.add(1, self._attrs(msg))

	def _on_started(self, msg: Message) -> None:
		self._inflight_ids.add(msg.id)
		self._in_flight.add(1, self._attrs(msg))

	def _on_succeeded(self, msg: Message, elapsed: float) -> None:
		self._duration.record(elapsed, self._attrs(msg))
		self._processed.add(1, self._attrs(msg, status="ok"))
		self._settle(msg)

	def _on_failed(self, msg: Message, exc: BaseException) -> None:
		self._processed.add(1, self._attrs(msg, status="error"))
		self._settle(msg)

	def _on_retried(self, msg: Message, delay: float) -> None:
		self._retried.add(1, self._attrs(msg))
		self._settle(msg)

	def _on_dead(self, msg: Message) -> None:
		self._processed.add(1, self._attrs(msg, status="dead"))
		self._settle(msg)

	def disconnect(self) -> None:
		ev = self._app.events
		ev.task_enqueued.disconnect(self._on_enqueued)
		ev.task_started.disconnect(self._on_started)
		ev.task_succeeded.disconnect(self._on_succeeded)
		ev.task_failed.disconnect(self._on_failed)
		ev.task_retried.disconnect(self._on_retried)
		ev.task_dead.disconnect(self._on_dead)


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
			log.debug("No log exporter or provider supplied; skipping OTel logging bridge")
			return logging.getLogger(self._logger_name)

		self._handler = LoggingHandler(level=self._level, logger_provider=self._provider)
		logger = logging.getLogger(self._logger_name)
		logger.addHandler(self._handler)
		log.info("OTel logging bridge attached to logger %r", self._logger_name)
		return logger

	def shutdown(self) -> None:
		if self._handler is not None:
			logging.getLogger(self._logger_name).removeHandler(self._handler)
			self._handler = None
		if self._owns_provider and self._provider is not None:
			self._provider.shutdown()
		self._provider = None


def _auto_setup_sdk(
	endpoint: str | None = None,
	*,
	span_exporter: SpanExporter | None = None,
	metric_exporter: MetricExporter | None = None,
) -> Resource | None:
	endpoint = endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if span_exporter is None and metric_exporter is None and not endpoint:
		log.debug("OTEL_EXPORTER_OTLP_ENDPOINT not set; skipping auto-setup")
		return None

	service_name = os.getenv("OTEL_SERVICE_NAME", "kuu")
	resource = Resource.create({"service.name": service_name})
	log.info("OTel auto-setup: service=%s", service_name)

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
		self._span_exporter = span_exporter
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
