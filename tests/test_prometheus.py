from __future__ import annotations

from prometheus_client import CollectorRegistry

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.message import Message, Payload
from kuu.prometheus import WorkerMetrics


def _msg() -> Message:
	return Message(task="t", queue="q", payload=Payload())


def _val(metric, **labels) -> float:
	return metric.labels(**labels)._value.get()


async def test_success_path_increments_counters_and_settles_in_flight():
	app = Kuu(broker=MemoryBroker())
	m = WorkerMetrics(app, registry=CollectorRegistry())

	msg = _msg()
	await app.events.task_received.send(msg)
	await app.events.task_started.send(msg)
	await app.events.task_succeeded.send(msg, 0.123)

	assert _val(m.received, task="t", queue="q") == 1
	assert _val(m.started, task="t", queue="q") == 1
	assert _val(m.succeeded, task="t", queue="q") == 1
	assert _val(m.in_flight, task="t", queue="q") == 0
	assert m.duration.labels("t", "q")._sum.get() > 0


async def test_fail_then_dead_decrements_in_flight_only_once():
	app = Kuu(broker=MemoryBroker())
	m = WorkerMetrics(app, registry=CollectorRegistry())

	msg = _msg()
	await app.events.task_started.send(msg)
	exc = RuntimeError("boom")
	await app.events.task_failed.send(msg, exc)
	await app.events.task_dead.send(msg)

	assert _val(m.in_flight, task="t", queue="q") == 0
	assert _val(m.failed, task="t", queue="q", exc="RuntimeError") == 1
	assert _val(m.dead, task="t", queue="q") == 1


async def test_reject_path_dead_alone_still_settles_in_flight():
	app = Kuu(broker=MemoryBroker())
	m = WorkerMetrics(app, registry=CollectorRegistry())

	msg = _msg()
	await app.events.task_started.send(msg)
	await app.events.task_dead.send(msg)

	assert _val(m.in_flight, task="t", queue="q") == 0
	assert _val(m.dead, task="t", queue="q") == 1


async def test_retry_records_delay_histogram():
	app = Kuu(broker=MemoryBroker())
	m = WorkerMetrics(app, registry=CollectorRegistry())

	msg = _msg()
	await app.events.task_started.send(msg)
	await app.events.task_retried.send(msg, 5.0)

	assert _val(m.retried, task="t", queue="q") == 1
	assert _val(m.in_flight, task="t", queue="q") == 0
	assert m.retry_delay.labels("t", "q")._sum.get() == 5.0
