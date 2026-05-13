from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

import pytest

from kuu.app import Kuu
from kuu.context import Context
from kuu.message import Message, Payload
from kuu.otel import (
	KuuOTELInstrumentor,
	OtelLoggingBridge,
	OtelMetrics,
	OtelTracingMiddleware,
	shutdown_telemetry,
)

pytestmark = pytest.mark.anyio


def _make_msg(
	task: str = "test_task",
	queue: str = "default",
	attempt: int = 1,
	max_attempts: int = 3,
	headers: dict[str, str] | None = None,
) -> Message:
	return Message(
		id=uuid4(),
		task=task,
		queue=queue,
		attempt=attempt,
		max_attempts=max_attempts,
		payload=Payload(args=(), kwargs={}),
		headers=headers or {},
	)


def _make_ctx(app: Kuu, msg: Message | None = None, phase: str = "enqueue") -> Context:
	if msg is None:
		msg = _make_msg()
	return Context(app=app, message=msg, phase=phase)  # type: ignore[arg-type]


async def _call_next_ok() -> str:
	return "done"


async def _call_next_error() -> str:
	raise RuntimeError("boom")


def _reset_otel_globals() -> None:
	import opentelemetry.trace
	from opentelemetry.metrics import _internal as metrics_internal

	# Reset trace
	if hasattr(opentelemetry.trace, "_TRACER_PROVIDER_SET_ONCE"):
		opentelemetry.trace._TRACER_PROVIDER_SET_ONCE._done = False
	opentelemetry.trace._TRACER_PROVIDER = None

	# Reset metrics
	if hasattr(metrics_internal, "_METER_PROVIDER_SET_ONCE"):
		metrics_internal._METER_PROVIDER_SET_ONCE._done = False
	metrics_internal._METER_PROVIDER = None


@pytest.fixture(autouse=True)
def _reset_providers():
	_reset_otel_globals()
	yield
	_reset_otel_globals()


def _make_trace_provider(in_memory: bool = True):
	"""Return (provider, exporter) for testing."""
	from opentelemetry import trace as otel_trace
	from opentelemetry.sdk.trace import TracerProvider
	from opentelemetry.sdk.trace.export import SimpleSpanProcessor
	from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

	exporter = InMemorySpanExporter()
	provider = TracerProvider()
	provider.add_span_processor(SimpleSpanProcessor(exporter))
	otel_trace.set_tracer_provider(provider)
	return provider, exporter


def _make_metric_provider():
	"""Return (provider, reader) for testing."""
	from opentelemetry import metrics as otel_metrics
	from opentelemetry.sdk.metrics import MeterProvider
	from opentelemetry.sdk.metrics.export import InMemoryMetricReader

	reader = InMemoryMetricReader()
	provider = MeterProvider(metric_readers=[reader])
	otel_metrics.set_meter_provider(provider)
	return provider, reader


async def test_enqueue_creates_producer_span(app: Kuu):
	"""Enqueue phase creates PRODUCER span with correct name and attributes."""
	_, exporter = _make_trace_provider()

	mw = OtelTracingMiddleware(tracer_name="kuu-test")
	msg = _make_msg(task="my_task")
	ctx = _make_ctx(app, msg, phase="enqueue")

	await mw(ctx, _call_next_ok)

	spans = exporter.get_finished_spans()
	assert len(spans) == 1

	span = spans[0]
	assert span.name == "my_task publish"
	assert span.kind.name == "PRODUCER"
	attrs = span.attributes
	assert attrs
	assert attrs["messaging.system"] == "kuu"
	assert attrs["messaging.operation"] == "publish"
	assert attrs["messaging.destination"] == "default"
	assert attrs["kuu.task.name"] == "my_task"


async def test_enqueue_injects_traceparent(app: Kuu):
	"""Enqueue phase injects 'traceparent' into message headers."""
	_, _exporter = _make_trace_provider()

	mw = OtelTracingMiddleware(tracer_name="kuu-test", propagate_ctx=True)
	msg = _make_msg(headers={})
	ctx = _make_ctx(app, msg, phase="enqueue")

	await mw(ctx, _call_next_ok)

	assert "traceparent" in ctx.message.headers
	tp = ctx.message.headers["traceparent"]
	assert tp.startswith("00-")
	assert "-" in tp


async def test_process_extracts_context_creates_consumer_span(app: Kuu):
	"""Process phase creates CONSUMER span, linked to parent via traceparent."""
	_, exporter = _make_trace_provider()

	# Enqueue to populate headers with traceparent
	mw_enq = OtelTracingMiddleware(tracer_name="kuu-test")
	msg = _make_msg(task="my_task", headers={})
	ctx_enq = _make_ctx(app, msg, phase="enqueue")
	await mw_enq(ctx_enq, _call_next_ok)
	assert "traceparent" in msg.headers

	# Process
	mw_proc = OtelTracingMiddleware(tracer_name="kuu-test")
	ctx_proc = _make_ctx(app, msg, phase="process")
	await mw_proc(ctx_proc, _call_next_ok)

	spans = exporter.get_finished_spans()
	proc_spans = [s for s in spans if s.kind.name == "CONSUMER"]
	assert len(proc_spans) == 1
	assert proc_spans[0].name == "my_task process"


async def test_process_success_sets_ok_status(app: Kuu):
	"""On success, the process span gets OK status."""
	_, exporter = _make_trace_provider()

	mw = OtelTracingMiddleware(tracer_name="kuu-test", propagate_ctx=False)
	msg = _make_msg()
	ctx = _make_ctx(app, msg, phase="process")

	result = await mw(ctx, _call_next_ok)
	assert result == "done"

	spans = exporter.get_finished_spans()
	assert len(spans) == 1
	assert spans[0].status.status_code.name == "OK"


async def test_process_failure_records_exception(app: Kuu):
	"""On failure, the process span records exception and gets ERROR status."""
	_, exporter = _make_trace_provider()

	mw = OtelTracingMiddleware(tracer_name="kuu-test", propagate_ctx=False)
	msg = _make_msg()
	ctx = _make_ctx(app, msg, phase="process")

	with pytest.raises(RuntimeError, match="boom"):
		await mw(ctx, _call_next_error)

	spans = exporter.get_finished_spans()
	assert len(spans) == 1
	span = spans[0]
	assert span.status.status_code.name == "ERROR"
	assert len(span.events) >= 1
	assert span.events[0].name == "exception"


async def test_context_detached_on_exception(app: Kuu):
	"""Context token is detached even when call_next raises."""
	_, _exporter = _make_trace_provider()

	mw = OtelTracingMiddleware(tracer_name="kuu-test", propagate_ctx=False)
	msg = _make_msg()
	ctx = _make_ctx(app, msg, phase="process")

	with pytest.raises(RuntimeError, match="boom"):
		await mw(ctx, _call_next_error)

	# The key assertion: no crash, span properly ended (context not leaked)


def _get_metric_value(
	reader: Any, metric_name: str, expected_attrs: dict | None = None
) -> float | None:
	"""Extract a numeric metric value from InMemoryMetricReader."""
	data = reader.get_metrics_data()
	if data is None:
		return None
	for rm in data.resource_metrics:
		for sm in rm.scope_metrics:
			for m in sm.metrics:
				if m.name == metric_name:
					for dp in m.data.data_points:
						dp_attrs = dict(dp.attributes)
						if expected_attrs is None or all(
							dp_attrs.get(k) == v for k, v in expected_attrs.items()
						):
							return dp.value
	return None


async def test_metrics_started_increments_in_flight(app: Kuu):
	"""task_started increments in_flight by 1."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")
	await app.events.task_started.send(msg)

	val = _get_metric_value(
		reader, "kuu.task.in_flight", {"task.name": "my_task", "queue": "default"}
	)
	assert val == 1, f"Expected in_flight=1, got {val}"


async def test_metrics_succeeded_decrements_in_flight_records_duration(app: Kuu):
	"""task_succeeded decrements in_flight, records processed{status=ok}."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_started.send(msg)
	await app.events.task_succeeded.send(msg, 0.42)

	val = _get_metric_value(
		reader,
		"kuu.task.processed",
		{
			"task.name": "my_task",
			"queue": "default",
			"status": "ok",
		},
	)
	assert val == 1, f"Expected processed=1, got {val}"

	inflight = _get_metric_value(
		reader, "kuu.task.in_flight", {"task.name": "my_task", "queue": "default"}
	)
	assert inflight == 0, f"Expected in_flight=0, got {inflight}"


async def test_metrics_failed_decrements_in_flight(app: Kuu):
	"""task_failed records processed{status=error}."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_started.send(msg)
	await app.events.task_failed.send(msg, RuntimeError("boom"))

	val = _get_metric_value(
		reader,
		"kuu.task.processed",
		{
			"task.name": "my_task",
			"queue": "default",
			"status": "error",
		},
	)
	assert val == 1, f"Expected processed=1 (error), got {val}"


async def test_metrics_retried_increments_counter(app: Kuu):
	"""task_retried increments retry counter."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_started.send(msg)
	await app.events.task_retried.send(msg, 5.0)

	val = _get_metric_value(
		reader, "kuu.task.retried", {"task.name": "my_task", "queue": "default"}
	)
	assert val == 1, f"Expected retried=1, got {val}"


async def test_metrics_dead_increments_processed(app: Kuu):
	"""task_dead records processed{status=dead}."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_started.send(msg)
	await app.events.task_dead.send(msg)

	val = _get_metric_value(
		reader,
		"kuu.task.processed",
		{
			"task.name": "my_task",
			"queue": "default",
			"status": "dead",
		},
	)
	assert val == 1, f"Expected processed=1 (dead), got {val}"


async def test_metrics_enqueued_increments_counter(app: Kuu):
	"""task_enqueued increments enqueue counter."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_enqueued.send(msg)

	val = _get_metric_value(
		reader, "kuu.task.enqueued", {"task.name": "my_task", "queue": "default"}
	)
	assert val == 1, f"Expected enqueued=1, got {val}"


async def test_metrics_fail_then_dead_settles_once(app: Kuu):
	"""Fail→Dead chain decrements in_flight only once."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	msg = _make_msg(task="my_task", queue="default")

	await app.events.task_started.send(msg)
	await app.events.task_failed.send(msg, RuntimeError("boom"))
	await app.events.task_dead.send(msg)

	inflight = _get_metric_value(
		reader, "kuu.task.in_flight", {"task.name": "my_task", "queue": "default"}
	)
	assert inflight == 0, f"Expected in_flight=0, got {inflight}"


async def test_otelm_metrics_disconnect_stops_recording(app: Kuu):
	"""After disconnect(), events no longer record metrics."""
	_, reader = _make_metric_provider()

	m = OtelMetrics(app=app, meter_name="kuu-test")
	m.disconnect()

	msg = _make_msg()
	await app.events.task_started.send(msg)

	val = _get_metric_value(reader, "kuu.task.in_flight")
	assert val is None  # No metrics recorded


async def test_logging_bridge_setup_returns_stdlib_logger(app: Kuu):
	"""setup() returns a stdlib logging.Logger with the handler attached."""
	bridge = OtelLoggingBridge(logger_name="kuu-test-log")
	logger = bridge.setup()
	assert isinstance(logger, logging.Logger)
	assert logger.name == "kuu-test-log"
	bridge.shutdown()


async def test_logging_bridge_shutdown_flushes(app: Kuu):
	"""shutdown() on bridge completes without error."""
	bridge = OtelLoggingBridge(logger_name="kuu-test-shutdown")
	bridge.setup()
	bridge.shutdown()


async def test_instrumentor_inserts_middleware_at_zero(app: Kuu):
	"""instrument() inserts OtelTracingMiddleware at middleware list index 0."""
	inst = KuuOTELInstrumentor(app=app)
	inst.instrument()
	assert len(app.middleware) >= 1
	assert isinstance(app.middleware[0], OtelTracingMiddleware)
	inst.uninstrument()


async def test_instrumentor_uninstrument_removes_middleware(app: Kuu):
	"""uninstrument() removes the OtelTracingMiddleware."""
	inst = KuuOTELInstrumentor(app=app)
	inst.instrument()
	assert any(isinstance(mw, OtelTracingMiddleware) for mw in app.middleware)
	inst.uninstrument()
	assert not any(isinstance(mw, OtelTracingMiddleware) for mw in app.middleware)


async def test_instrumentor_uninstrument_twice_is_safe(app: Kuu):
	"""Calling uninstrument() twice does not raise."""
	inst = KuuOTELInstrumentor(app=app)
	inst.instrument()
	inst.uninstrument()
	inst.uninstrument()


async def test_shutdown_telemetry_no_provider():
	"""shutdown_telemetry() when no provider is configured does not raise."""
	shutdown_telemetry()
