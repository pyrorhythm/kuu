from __future__ import annotations

import inspect
import typing
from typing import Any

from msgspec import json as _msgjson

_enc = _msgjson.Encoder()
_dec = _msgjson.Decoder()


def sig_params(fn: Any) -> tuple[list[dict], bool]:
	sig = inspect.signature(fn)
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	params: list[dict] = []
	has_varargs = False
	for name, param in sig.parameters.items():
		if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
			has_varargs = True
			continue
		ann = hints.get(name, param.annotation)
		if ann is inspect.Parameter.empty:
			ann_str: str | None = None
		elif isinstance(ann, type):
			ann_str = ann.__name__
		else:
			ann_str = str(ann).replace("typing.", "")
		required = param.default is inspect.Parameter.empty
		default: Any = None
		if not required:
			try:
				default = _dec.decode(_enc.encode(param.default))
			except Exception:
				default = repr(param.default)
		params.append(
			{
				"name": name,
				"annotation": ann_str,
				"default": default,
				"required": required,
			}
		)
	return params, has_varargs


__all__ = ["sig_params"]
