from __future__ import annotations

from msgspec import json as _json

from kuu.marshal import marshal as _m
from kuu.observability._protocol import Envelope

_decoder = _json.Decoder(Envelope, dec_hook=_m._dec_hook)


def envelope_to_bytes(env: Envelope) -> bytes:
	return _m.json_encode(env)


def envelope_from_bytes(data: bytes | str) -> Envelope:
	if isinstance(data, str):
		data = data.encode()
	return _decoder.decode(data)


__all__ = ["envelope_to_bytes", "envelope_from_bytes"]
