from __future__ import annotations

import inspect
import threading
from contextvars import ContextVar
from typing import Any

import anyio
import pytest
from dishka import (
	Provider,
	Scope,
	make_async_container,
	make_container,
	provide,
)

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Settings
from kuu.context import Context
from kuu.contrib.dishka import (
	CONTAINER_STATE_KEY,
	FromDishka,
	KuuDishkaMiddleware,
	current_container,
	from_contextvar,
	inject,
	setup_dishka,
)
from kuu.contrib.dishka import _request_container  # type: ignore[attr-defined]
from kuu.message import Message, Payload
from kuu.middleware.base import Next, run_chain
from kuu.worker import Worker
from kuu._types import _coerce_payload

pytestmark = pytest.mark.anyio


# --------------------------------------------------------------------------- #
# Services / providers
# --------------------------------------------------------------------------- #
class ReadService:
	tag = "read"


class WriteService:
	tag = "write"


class AppProvider(Provider):
	scope = Scope.REQUEST

	read = provide(ReadService)
	write = provide(WriteService)


class CountingProvider(Provider):
	"""Counts how many ReadService instances are created (scope freshness)."""

	scope = Scope.REQUEST
	instances = 0

	@provide
	def read(self) -> ReadService:
		CountingProvider.instances += 1
		return ReadService()


@pytest.fixture
def async_container():
	c = make_async_container(AppProvider())
	yield c


@pytest.fixture
def sync_container():
	c = make_container(AppProvider())
	yield c
	c.close()


# --------------------------------------------------------------------------- #
# FromDishka marker + inject signature rewriting (pure unit)
# --------------------------------------------------------------------------- #
def test_fromdishka_returns_marker_with_default_component():
	from kuu.contrib.dishka import _FromDishkaMarker  # type: ignore[attr-defined]

	marker = FromDishka()
	assert isinstance(marker, _FromDishkaMarker)
	assert marker.component == ""


def test_fromdishka_carries_custom_component():
	marker = FromDishka(component="secondary")
	assert marker.component == "secondary"


def test_inject_strips_di_params_from_signature_async():
	@inject
	async def handler(
		lot_id: str,
		read: ReadService = FromDishka(),
		write: WriteService = FromDishka(),
		fetch: bool = True,
	) -> str:
		return f"{lot_id}:{read.tag}:{write.tag}:{fetch}"

	params = list(inspect.signature(handler).parameters)
	assert params == ["lot_id", "fetch"]
	# the cleaned signature must still bind a payload that omits the deps
	inspect.signature(handler).bind("lot-1", fetch=False)


def test_inject_strips_di_params_from_signature_sync():
	@inject
	def handler(lot_id: str, read: ReadService = FromDishka()) -> str:
		return f"{lot_id}:{read.tag}"

	assert list(inspect.signature(handler).parameters) == ["lot_id"]


def test_inject_preserves_asyncness():
	@inject
	async def a(read: ReadService = FromDishka()) -> None: ...

	@inject
	def s(read: ReadService = FromDishka()) -> None: ...

	assert inspect.iscoroutinefunction(a)
	assert not inspect.iscoroutinefunction(s)


def test_inject_is_noop_without_dependencies():
	async def plain(x: int) -> int:
		return x

	wrapped = inject(plain)
	assert list(inspect.signature(wrapped).parameters) == ["x"]


def test_inject_factory_form_returns_decorator():
	var: ContextVar[Any] = ContextVar("v", default=None)
	deco = inject(container_getter=from_contextvar(var))
	assert callable(deco)

	@deco
	async def handler(read: ReadService = FromDishka()) -> None: ...

	assert list(inspect.signature(handler).parameters) == []


# --------------------------------------------------------------------------- #
# current_container / from_contextvar error paths
# --------------------------------------------------------------------------- #
def test_current_container_raises_without_container():
	with pytest.raises(LookupError, match="No Dishka REQUEST container"):
		current_container()


def test_from_contextvar_raises_when_unset():
	var: ContextVar[Any] = ContextVar("empty", default=None)
	getter = from_contextvar(var)
	with pytest.raises(LookupError, match="holds no Dishka container"):
		getter()


async def test_from_contextvar_returns_set_container(async_container):
	var: ContextVar[Any] = ContextVar("set", default=None)
	async with async_container() as request:
		var.set(request)
		assert from_contextvar(var)() is request


# --------------------------------------------------------------------------- #
# KuuDishkaMiddleware construction
# --------------------------------------------------------------------------- #
def test_middleware_requires_a_container():
	with pytest.raises(ValueError, match="at least one"):
		KuuDishkaMiddleware()


def test_setup_dishka_inserts_middleware_first(async_container):
	app = Kuu(broker=MemoryBroker())

	async def existing(ctx: Context, call_next: Next) -> Any:  # pre-existing middleware
		return await call_next()

	app.middleware.append(existing)
	mw = setup_dishka(app, async_container)
	assert app.middleware[0] is mw
	assert existing in app.middleware


# --------------------------------------------------------------------------- #
# Middleware behaviour via run_chain (faithful, fast)
# --------------------------------------------------------------------------- #
def _ctx(app: Kuu, task: Any, payload: Payload) -> Context:
	msg = Message(task=task.task_name, queue=task.task_queue, payload=payload)
	return Context(app=app, message=msg, phase="process", task=task)


async def _terminal_for(task: Any):
	"""Mirror Worker._handle_inner terminal for a single task."""

	async def _terminal(c: Context) -> Any:
		p = _coerce_payload(task.original_func, c.message.payload)
		if task.blocking:
			return await anyio.to_thread.run_sync(
				lambda: task.original_func(*p.args, **p.kwargs), abandon_on_cancel=True
			)
		r = task.original_func(*p.args, **p.kwargs)
		return await r if inspect.isawaitable(r) else r

	return _terminal


async def test_async_injection_via_middleware(async_container):
	app = Kuu(broker=MemoryBroker())

	@app.task
	@inject
	async def handler(
		lot_id: str,
		read: ReadService = FromDishka(),
		write: WriteService = FromDishka(),
		fetch: bool = True,
	) -> str:
		return f"{lot_id}:{read.tag}:{write.tag}:{fetch}"

	mw = KuuDishkaMiddleware(async_container)
	ctx = _ctx(app, handler, Payload(args=("lot-1",), kwargs={"fetch": False}))
	result = await run_chain(ctx, [mw], await _terminal_for(handler))

	assert result == "lot-1:read:write:False"
	# request container was exposed in ctx.state
	assert CONTAINER_STATE_KEY in ctx.state


async def test_sync_injection_runs_off_thread(sync_container):
	app = Kuu(broker=MemoryBroker())
	main_thread = threading.get_ident()
	seen: dict[str, Any] = {}

	@app.task(blocking=True)
	@inject
	def handler(lot_id: str, read: ReadService = FromDishka()) -> str:
		seen["thread"] = threading.get_ident()
		return f"{lot_id}:{read.tag}"

	mw = KuuDishkaMiddleware(sync_container=sync_container)
	ctx = _ctx(app, handler, Payload(args=("lot-2",)))
	result = await run_chain(ctx, [mw], await _terminal_for(handler))

	assert result == "lot-2:read"
	assert seen["thread"] != main_thread


async def test_middleware_resets_contextvar_after_task(async_container):
	app = Kuu(broker=MemoryBroker())

	@app.task
	@inject
	async def handler(read: ReadService = FromDishka()) -> str:
		return read.tag

	mw = KuuDishkaMiddleware(async_container)
	ctx = _ctx(app, handler, Payload())
	await run_chain(ctx, [mw], await _terminal_for(handler))

	# contextvar must be cleared once the task is done
	assert _request_container.get() is None


async def test_middleware_uses_custom_contextvar(async_container):
	app = Kuu(broker=MemoryBroker())
	my_var: ContextVar[Any] = ContextVar("my_ioc", default=None)

	@app.task
	@inject(container_getter=from_contextvar(my_var))
	async def handler(read: ReadService = FromDishka()) -> str:
		return read.tag

	mw = KuuDishkaMiddleware(async_container, context_var=my_var)
	ctx = _ctx(app, handler, Payload())
	result = await run_chain(ctx, [mw], await _terminal_for(handler))

	assert result == "read"
	# built-in slot stays untouched; custom var is reset afterwards
	assert _request_container.get() is None
	assert my_var.get() is None


async def test_middleware_passes_through_enqueue_phase(async_container):
	app = Kuu(broker=MemoryBroker())
	mw = KuuDishkaMiddleware(async_container)
	msg = Message(task="t", queue="default", payload=Payload())
	ctx = Context(app=app, message=msg, phase="enqueue", task=None)

	called = False

	async def _next() -> str:
		nonlocal called
		called = True
		return "ok"

	result = await mw(ctx, _next)
	assert result == "ok" and called
	assert CONTAINER_STATE_KEY not in ctx.state


async def test_middleware_raises_when_required_container_missing(async_container):
	"""A blocking task with only an async container available is misconfigured."""
	app = Kuu(broker=MemoryBroker())

	@app.task(blocking=True)
	@inject
	def handler(read: ReadService = FromDishka()) -> str:
		return read.tag

	mw = KuuDishkaMiddleware(async_container)  # no sync_container
	ctx = _ctx(app, handler, Payload())

	async def _next() -> None: ...

	with pytest.raises(LookupError, match="needs a sync Dishka container"):
		await mw(ctx, _next)


async def test_middleware_raises_when_async_container_missing(sync_container):
	"""An async task with only a sync container available is misconfigured."""
	app = Kuu(broker=MemoryBroker())

	@app.task
	@inject
	async def handler(read: ReadService = FromDishka()) -> str:
		return read.tag

	mw = KuuDishkaMiddleware(sync_container=sync_container)  # no async container
	ctx = _ctx(app, handler, Payload())

	async def _next() -> None: ...

	with pytest.raises(LookupError, match="needs an async Dishka container"):
		await mw(ctx, _next)


async def test_request_scope_is_fresh_per_task(async_container):
	app = Kuu(broker=MemoryBroker())
	container = make_async_container(CountingProvider())
	CountingProvider.instances = 0

	@app.task
	@inject
	async def handler(read: ReadService = FromDishka()) -> str:
		return read.tag

	mw = KuuDishkaMiddleware(container)
	for _ in range(3):
		ctx = _ctx(app, handler, Payload())
		await run_chain(ctx, [mw], await _terminal_for(handler))

	assert CountingProvider.instances == 3
	await container.close()


# --------------------------------------------------------------------------- #
# Full end-to-end through the real Worker (.q -> broker -> worker -> inject)
# --------------------------------------------------------------------------- #
async def _run_worker_until(app: Kuu, predicate, *, timeout: float = 3.0) -> None:
	config = Settings(app="test:app", task_modules=["t"], queues=["default"], concurrency=4)
	worker = Worker(config, app=app)

	async def _supervise(scope: anyio.CancelScope):
		while not predicate():
			await anyio.sleep(0.02)
		scope.cancel()

	with anyio.fail_after(timeout):
		async with anyio.create_task_group() as tg:
			tg.start_soon(_supervise, tg.cancel_scope)
			tg.start_soon(worker.run)


async def test_end_to_end_async_task(async_container):
	app = Kuu(broker=MemoryBroker())
	setup_dishka(app, async_container)
	captured: dict[str, Any] = {}

	@app.task
	@inject
	async def get_lot(
		lot_id: str,
		read: ReadService = FromDishka(),
		write: WriteService = FromDishka(),
		fetch: bool = True,
	) -> str:
		captured["result"] = f"{lot_id}:{read.tag}:{write.tag}:{fetch}"
		return captured["result"]

	# .q omits read/write entirely
	await get_lot.q("lot-1", fetch=False)
	await _run_worker_until(app, lambda: "result" in captured)

	assert captured["result"] == "lot-1:read:write:False"


async def test_end_to_end_blocking_task(sync_container):
	app = Kuu(broker=MemoryBroker())
	setup_dishka(app, None, sync_container=sync_container)
	captured: dict[str, Any] = {}

	@app.task(blocking=True)
	@inject
	def get_lot(lot_id: str, read: ReadService = FromDishka()) -> str:
		captured["result"] = f"{lot_id}:{read.tag}"
		return captured["result"]

	await get_lot.q("lot-9")
	await _run_worker_until(app, lambda: "result" in captured)

	assert captured["result"] == "lot-9:read"
