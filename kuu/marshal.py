from __future__ import annotations

from typing import Any, Callable

from adaptix import Retort
from msgspec import json as _json
from msgspec import msgpack as _msgpack

EncHook = Callable[[Any], Any]
DecHook = Callable[[type, Any], Any]


class Marshal:
	def __init__(self) -> None:
		self._enc_table: list[tuple[type, EncHook]] = []
		self._dec_table: dict[type, DecHook] = {}
		self._json_enc: _json.Encoder | None = None
		self._msgpack_enc: _msgpack.Encoder | None = None
		self._retort: Any = None

	def register(
		self,
		typ: type,
		enc: EncHook | None = None,
		dec: DecHook | None = None,
	) -> None:
		if enc is not None:
			self._enc_table = [(t, f) for (t, f) in self._enc_table if t is not typ]
			self._enc_table.append((typ, enc))
			self._json_enc = None
			self._msgpack_enc = None
		if dec is not None:
			self._dec_table[typ] = dec

	def unregister(self, typ: type) -> None:
		self._enc_table = [(t, f) for (t, f) in self._enc_table if t is not typ]
		self._dec_table.pop(typ, None)
		self._json_enc = None
		self._msgpack_enc = None

	def _enc_hook(self, obj: Any) -> Any:
		for t, fn in self._enc_table:
			if isinstance(obj, t):
				return fn(obj)
		retort = self._retort
		if retort is None:
			retort = self._retort = Retort()
		try:
			return retort.dump(obj)
		except Exception as exc:
			raise TypeError(
				f"no encoder registered for {type(obj).__name__} and adaptix fallback failed: {exc}"
			) from exc

	def _dec_hook(self, typ: type, obj: Any) -> Any:
		fn = self._dec_table.get(typ)
		if fn is not None:
			return fn(typ, obj)
		raise TypeError(f"no decoder registered for {typ.__name__}")

	def json_encode(self, obj: Any) -> bytes:
		enc = self._json_enc
		if enc is None:
			enc = self._json_enc = _json.Encoder(enc_hook=self._enc_hook)
		return enc.encode(obj)

	def json_decode[T](self, data: bytes | str, type: type[T] | None = None) -> T | Any:
		if type is None:
			return _json.decode(data, dec_hook=self._dec_hook)
		return _json.decode(data, type=type, dec_hook=self._dec_hook)

	def json_encode_str(self, obj: Any) -> str:
		enc = self._json_enc
		if enc is None:
			enc = self._json_enc = _json.Encoder(enc_hook=self._enc_hook)
		return enc.encode(obj).decode()

	def msgpack_encode(self, obj: Any) -> bytes:
		enc = self._msgpack_enc
		if enc is None:
			enc = self._msgpack_enc = _msgpack.Encoder(enc_hook=self._enc_hook)
		return enc.encode(obj)

	def msgpack_decode[T](self, data: bytes, type: type[T] | None = None) -> T | Any:
		if type is None:
			return _msgpack.decode(data, dec_hook=self._dec_hook)
		return _msgpack.decode(data, type=type, dec_hook=self._dec_hook)


marshal = Marshal()


def _canonicalize(obj: Any) -> Any:
	if isinstance(obj, dict):
		return {k: _canonicalize(obj[k]) for k in sorted(obj)}
	if isinstance(obj, (list, tuple)):
		return [_canonicalize(x) for x in obj]
	return obj


def canonical_json(obj: Any) -> bytes:
	return marshal.json_encode(_canonicalize(obj))


__all__ = ["Marshal", "marshal", "canonical_json", "EncHook", "DecHook"]
