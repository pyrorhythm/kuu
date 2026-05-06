from __future__ import annotations

import msgspec
import pytest

from kuu.message import Message, Payload


def test_payload_is_frozen_so_middleware_cannot_mutate_in_place():
	p = Payload(args=(1, 2), kwargs={"a": 3})
	with pytest.raises(AttributeError):
		p.args = (9,)
	with pytest.raises(AttributeError):
		p.kwargs = {}


def test_message_is_frozen_so_envelope_cannot_be_swapped_after_creation():
	m = Message(task="t", queue="q", payload=Payload())
	with pytest.raises(AttributeError):
		m.payload = Payload(args=(666,))
	with pytest.raises(AttributeError):
		m.task = "other"


def test_payload_args_serialize_round_trip():
	m = Message(task="t", queue="q", payload=Payload(args=(1, "x", [2, 3]), kwargs={"k": True}))
	clone = msgspec.json.decode(msgspec.json.encode(m), type=Message)
	assert clone.payload.args == (1, "x", [2, 3])
	assert clone.payload.kwargs == {"k": True}
