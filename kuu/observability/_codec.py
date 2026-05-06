from __future__ import annotations

from dataclasses import asdict

import orjson

from kuu.observability._protocol import (
	Body,
	BrokerInfo,
	Bye,
	Envelope,
	Event,
	Hello,
	JobSnapshot,
	QueueSnapshot,
	State,
	WorkerSnapshot,
)

_BODY_TAGS: dict[type[Body], str] = {
	Hello: "hello",
	Event: "event",
	State: "state",
	Bye: "bye",
}


def envelope_to_bytes(env: Envelope) -> bytes:
	tag = _BODY_TAGS.get(type(env.body))
	if tag is None:
		raise TypeError(f"unsupported body type: {type(env.body).__name__}")
	return orjson.dumps(
		{
			"v": env.v,
			"t": tag,
			"instance": env.instance,
			"ts": env.ts,
			"body": asdict(env.body),
		}
	)


def envelope_from_bytes(data: bytes | str) -> Envelope:
	d = orjson.loads(data)
	tag = d.get("t")
	body_d = d.get("body") or {}
	body: Body
	match tag:
		case "hello":
			body = Hello(
				preset=body_d["preset"],
				host=body_d["host"],
				pid=body_d["pid"],
				version=body_d["version"],
				started_at=body_d["started_at"],
				broker=BrokerInfo(**body_d["broker"]),
				scheduler_enabled=body_d["scheduler_enabled"],
				processes=body_d["processes"],
			)
		case "event":
			body = Event(
				kind=body_d["kind"],
				task=body_d["task"],
				queue=body_d["queue"],
				worker_pid=body_d["worker_pid"],
				elapsed=body_d.get("elapsed"),
			)
		case "state":
			body = State(
				workers=[WorkerSnapshot(**w) for w in body_d.get("workers", [])],
				jobs=[JobSnapshot(**j) for j in body_d.get("jobs", [])],
				queues={k: QueueSnapshot(**v) for k, v in body_d.get("queues", {}).items()},
			)
		case "bye":
			body = Bye(reason=body_d["reason"])
		case _:
			raise ValueError(f"unknown envelope tag: {tag!r}")
	return Envelope(v=d["v"], instance=d["instance"], ts=d["ts"], body=body)


__all__ = ["envelope_to_bytes", "envelope_from_bytes"]
