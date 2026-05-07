from __future__ import annotations

import time
from datetime import datetime as dt
from datetime import timedelta
from datetime import timezone as tz

import pytest

from kuu.observability import (
	PROTOCOL_VERSION,
	BrokerInfo,
	Bye,
	Envelope,
	Event,
	Hello,
	InMemoryRegistry,
	JobSnapshot,
	QueueSnapshot,
	State,
	WorkerSnapshot,
	envelope_from_bytes,
	envelope_to_bytes,
)
from kuu.observability._broker_key import broker_key

pytestmark = pytest.mark.anyio


# === codec roundtrip


def _make_envelope(body) -> Envelope:
	return Envelope(v=PROTOCOL_VERSION, instance="i-1", ts=dt.now(tz=tz.utc), body=body)


def _hello() -> Hello:
	return Hello(
		preset="dev",
		host="h",
		pid=42,
		version="0.1.0",
		started_at=dt.now(tz=tz.utc) - timedelta(seconds=100),
		broker=BrokerInfo(type="MemoryBroker", key="abcd"),
		scheduler_enabled=False,
		processes=2,
	)


class TestCodec:
	@pytest.mark.parametrize(
		"body",
		[
			_hello(),
			Event(kind="succeeded", task="t", queue="q", worker_pid=10, elapsed=0.5),
			Event(kind="failed", task="t", queue="q", worker_pid=10),  # default elapsed=None
			State(),
			State(
				workers=[WorkerSnapshot(pid=1, alive=True), WorkerSnapshot(pid=2, alive=False)],
				jobs=[JobSnapshot(id="j1", task="tt", next_run=999.0)],
				queues={"q": QueueSnapshot(in_flight=3, depth=10)},
			),
			Bye(reason="manual"),
		],
	)
	def test_roundtrip(self, body) -> None:
		env = _make_envelope(body)
		data = envelope_to_bytes(env)
		assert isinstance(data, bytes)
		decoded = envelope_from_bytes(data)
		assert decoded == env
		assert type(decoded.body) is type(env.body)

	def test_decoder_rejects_unknown_tag(self) -> None:
		from msgspec import ValidationError
		from msgspec import json as _json

		data = _json.encode({"v": 1, "instance": "x", "ts": 0, "body": {"type": "garbage"}})
		with pytest.raises(ValidationError):
			envelope_from_bytes(data)

	def test_encoder_rejects_unknown_body(self) -> None:
		class NotABody:
			pass

		env = Envelope(v=1, instance="x", ts=0, body=NotABody())  # type: ignore[arg-type]
		with pytest.raises(TypeError):
			envelope_to_bytes(env)


# === in-memory registry


class TestInMemoryRegistry:
	def test_hello_creates_entry(self) -> None:
		reg = InMemoryRegistry()
		reg.ingest(_make_envelope(_hello()))
		entries = reg.all()
		assert len(entries) == 1
		assert entries[0].instance_id == "i-1"
		assert entries[0].hello.preset == "dev"
		assert entries[0].last_state is None

	def test_state_updates_existing(self) -> None:
		reg = InMemoryRegistry()
		reg.ingest(_make_envelope(_hello()))
		reg.ingest(_make_envelope(State(workers=[WorkerSnapshot(pid=1, alive=True)])))
		entry = reg.get("i-1")
		assert entry is not None
		assert entry.last_state is not None
		assert len(entry.last_state.workers) == 1

	def test_state_without_hello_is_ignored(self) -> None:
		reg = InMemoryRegistry()
		reg.ingest(_make_envelope(State()))
		assert reg.all() == []

	def test_bye_removes_entry(self) -> None:
		reg = InMemoryRegistry()
		reg.ingest(_make_envelope(_hello()))
		reg.ingest(_make_envelope(Bye(reason="manual")))
		assert reg.get("i-1") is None
		assert reg.all() == []

	def test_stale_eviction_on_read(self) -> None:
		reg = InMemoryRegistry(stale_after=timedelta(seconds=0.05))
		reg.ingest(_make_envelope(_hello()))
		assert reg.get("i-1") is not None
		time.sleep(0.1)
		assert reg.get("i-1") is None
		assert reg.all() == []

	def test_re_hello_preserves_last_state(self) -> None:
		"""same instance reconnecting (same id) keeps its prior state snapshot"""
		reg = InMemoryRegistry()
		reg.ingest(_make_envelope(_hello()))
		reg.ingest(_make_envelope(State(workers=[WorkerSnapshot(pid=9, alive=True)])))
		# producer reconnects and re-emits hello
		reg.ingest(_make_envelope(_hello()))
		entry = reg.get("i-1")
		assert entry is not None
		assert entry.last_state is not None
		assert entry.last_state.workers[0].pid == 9


# === broker_key invariants


class TestBrokerKey:
	def test_redis_url_creds_stripped(self) -> None:
		from kuu.brokers.redis import RedisBroker

		a = RedisBroker(url="redis://user:secret@h:6379/0")
		b = RedisBroker(url="redis://h:6379/0")
		assert broker_key(a) == broker_key(b)

	def test_distinct_for_different_endpoints(self) -> None:
		from kuu.brokers.redis import RedisBroker

		a = RedisBroker(url="redis://h1:6379/0")
		b = RedisBroker(url="redis://h2:6379/0")
		assert broker_key(a) != broker_key(b)
