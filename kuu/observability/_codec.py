from __future__ import annotations

from msgspec import json as _msgjson

from kuu.observability._protocol import Envelope

_encoder = _msgjson.Encoder()
_decoder = _msgjson.Decoder(Envelope)


def envelope_to_bytes(env: Envelope) -> bytes:
	return _encoder.encode(env)


def envelope_from_bytes(data: bytes | str) -> Envelope:
	if isinstance(data, str):
		data = data.encode()
	return _decoder.decode(data)


__all__ = ["envelope_to_bytes", "envelope_from_bytes"]
