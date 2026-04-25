from __future__ import annotations

import pytest
from pydantic import ValidationError

from kuu.message import Message, Payload


def test_payload_is_frozen_so_middleware_cannot_mutate_in_place():
	p = Payload(args=(1, 2), kwargs={"a": 3})
	with pytest.raises(ValidationError):
		p.args = (9,)  # type: ignore[misc]
	with pytest.raises(ValidationError):
		p.kwargs = {}  # type: ignore[misc]


def test_message_is_frozen_so_envelope_cannot_be_swapped_after_creation():
	m = Message(task="t", queue="q", payload=Payload())
	with pytest.raises(ValidationError):
		m.payload = Payload(args=(666,))  # type: ignore[misc]
	with pytest.raises(ValidationError):
		m.task = "other"  # type: ignore[misc]


def test_payload_args_serialize_round_trip():
	# JSON round-trip should preserve args/kwargs faithfully (via Message dump)
	m = Message(task="t", queue="q", payload=Payload(args=(1, "x", [2, 3]), kwargs={"k": True}))
	clone = Message.model_validate_json(m.model_dump_json())
	assert clone.payload.args == (1, "x", [2, 3])
	assert clone.payload.kwargs == {"k": True}
