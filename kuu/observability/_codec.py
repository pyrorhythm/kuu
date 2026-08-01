from __future__ import annotations

from typing import cast

from msgspec import json as _json
from msgspec.structs import replace

from kuu.marshal import marshal as _m
from kuu.observability._commands import (
	Command,
	CommandFrame,
	CommandResponseFrame,
	Cmd,
	CmdResponse,
)
from kuu.observability._protocol import PROTOCOL_VERSION, Envelope, Event
from kuu.result import sanitize_remote_failure

_MAX_FRAME_BYTES = 16 * 1024 * 1024

_decoder = _json.Decoder(Envelope, dec_hook=_m._dec_hook)
_command_decoder = _json.Decoder(CommandFrame, dec_hook=_m._dec_hook)
_response_decoder = _json.Decoder(CommandResponseFrame, dec_hook=_m._dec_hook)


def envelope_to_bytes(env: Envelope) -> bytes:
	return _m.json_encode(env)


def _bytes(data: bytes | str) -> bytes:
	raw = data.encode() if isinstance(data, str) else data
	if len(raw) > _MAX_FRAME_BYTES:
		raise ValueError(f"protocol frame exceeds {_MAX_FRAME_BYTES} bytes")
	return raw


def _check_version(version: int) -> None:
	if version != PROTOCOL_VERSION:
		raise ValueError(
			f"unsupported observability protocol v{version}; expected v{PROTOCOL_VERSION}"
		)


def envelope_from_bytes(data: bytes | str) -> Envelope:
	envelope = _decoder.decode(_bytes(data))
	_check_version(envelope.v)
	if isinstance(envelope.body, Event) and envelope.body.failure is not None:
		envelope = replace(
			envelope,
			body=replace(
				envelope.body,
				failure=sanitize_remote_failure(envelope.body.failure),
			),
		)
	return envelope


def command_to_bytes(command: Cmd) -> bytes:
	return _m.json_encode(CommandFrame(v=PROTOCOL_VERSION, command=cast(Command, command)))


def command_from_bytes(data: bytes | str) -> Command:
	frame = _command_decoder.decode(_bytes(data))
	_check_version(frame.v)
	return frame.command


def command_response_to_bytes(response: CmdResponse) -> bytes:
	return _m.json_encode(CommandResponseFrame(v=PROTOCOL_VERSION, response=response))


def command_response_from_bytes(data: bytes | str) -> CmdResponse:
	frame = _response_decoder.decode(_bytes(data))
	_check_version(frame.v)
	return frame.response


__all__ = [
	"envelope_to_bytes",
	"envelope_from_bytes",
	"command_to_bytes",
	"command_from_bytes",
	"command_response_to_bytes",
	"command_response_from_bytes",
]
