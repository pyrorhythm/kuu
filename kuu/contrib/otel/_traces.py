from __future__ import annotations

import typing
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer

if typing.TYPE_CHECKING:
	from kuu.context import Context
	from kuu.middleware.base import Next


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
		return await call_next()

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


__all__ = ("OtelTracingMiddleware",)
