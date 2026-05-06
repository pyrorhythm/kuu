from __future__ import annotations

import os
import pathlib
import tomllib
from pathlib import Path
from typing import Annotated, Any, Self

from msgspec import DecodeError as _DecodeError
from msgspec import Meta, Struct, convert, field
from msgspec import json as _json
from msgspec import to_builtins as _msgspec_to_builtins


def _enc_hook(obj: Any) -> Any:
	if isinstance(obj, Path):
		return str(obj)
	raise NotImplementedError(f"Cannot encode type {type(obj).__name__}")


def _dec_hook(typ: type, obj: Any) -> Any:
	if typ is Path:
		return Path(obj)
	raise NotImplementedError(f"Cannot decode type {typ.__name__}")


type Port = Annotated[int, Meta(ge=1, le=65535)]

_APP_PATTERN = (
	r"^[a-zA-Z_][a-zA-Z0-9_]*"
	r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*"
	r":[a-zA-Z_][a-zA-Z0-9_]*$"
)

_MODULE_PATTERN = (
	r"^[a-zA-Z_][a-zA-Z0-9_]*"
	r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
)


class SchedulerSettings(Struct, frozen=True):
	enable: bool = False


class MetricsSettings(Struct, frozen=True):
	enable: bool = False
	host: str = "0.0.0.0"
	port: Port = 9191


class WebSettings(Struct, frozen=True):
	enable: bool = False
	host: str = "0.0.0.0"
	port: Port = 8181
	path: str = "/dashboard"


class WatchSettings(Struct, frozen=True):
	enable: bool = False
	root: Path = field(default_factory=Path.cwd)
	respect_gitignore: bool = True
	exclude: list[Path] = field(default_factory=lambda: [Path(".git") / "**"])
	reload_delay: float = 0.25
	reload_debounce: float = 0.5


class PersistenceConfig(Struct, frozen=True):
	enable: bool = True
	dsn: str = field(
		default_factory=lambda: os.environ.get("KUU_PERSISTENCE_DSN", "sqlite:///./kuu.db")
	)
	schema: str | None = None
	runs_table: str = "kuu_runs"
	logs_table: str = "kuu_run_logs"
	keep_days: int = 7
	max_runs: int = 100_000
	log_level: str = "INFO"
	capture_args: bool = True


class Settings(Struct, frozen=True, forbid_unknown_fields=True):
	app: (
		Annotated[
			str,
			Meta(
				pattern=_APP_PATTERN,
				description="path to the location of `Kuu` instance formatted as `dotted.python_module[:instance]`",
			),
		]
		| None
	) = None
	task_modules: list[Annotated[str, Meta(pattern=_MODULE_PATTERN)]] = field(default_factory=list)
	queues: list[str] = field(default_factory=list)
	processes: Annotated[int, Meta(ge=1)] = 1
	concurrency: Annotated[int, Meta(ge=1)] = 64
	prefetch: Annotated[int, Meta(ge=0)] = 0
	shutdown_timeout: Annotated[float, Meta(ge=0)] = 30.0
	watch_fs: bool = False
	metrics: MetricsSettings = field(default_factory=MetricsSettings)
	dashboard: WebSettings = field(default_factory=WebSettings)
	watch: WatchSettings = field(default_factory=WatchSettings)
	scheduler: SchedulerSettings = field(default_factory=SchedulerSettings)
	persistence: PersistenceConfig = field(default_factory=PersistenceConfig)

	def with_overrides(self, overrides: list[str]) -> Self:
		if not overrides:
			return self
		data: dict[str, Any] = _msgspec_to_builtins(self, enc_hook=_enc_hook)
		for raw in overrides:
			key, sep, value = raw.partition("=")
			if not sep:
				raise ValueError(f"override must be in form 'dotted.path=value', got: {raw!r}")
			try:
				parsed: Any = _json.decode(value)
			except _DecodeError:
				parsed = value
			parts = key.split(".")
			cursor = data
			for part in parts[:-1]:
				existing = cursor.get(part)
				if not isinstance(existing, dict):
					existing = {}
					cursor[part] = existing
				cursor = existing
			cursor[parts[-1]] = parsed
		return convert(data, type(self), dec_hook=_dec_hook)  # type: ignore

	def to_dict(self) -> dict[str, Any]:
		return _msgspec_to_builtins(self, enc_hook=_enc_hook)


class Kuunfig(Struct, frozen=True, forbid_unknown_fields=True):
	"""Top-level configuration with defaults and named presets.

	.. code-block:: toml

		[default]
		app = "myapp:kuu"
		task_modules = ["myapp.tasks"]
		processes = 4
		concurrency = 64

		[presets.prod]
		processes = 8

		[presets.dev]
		processes = 1
		concurrency = 16

	Use :meth:`resolve` to merge a named preset with defaults:

		>>> cfg = Kuunfig.load()
		>>> settings = cfg.resolve("prod")
	"""

	default: Settings
	presets: dict[str, dict[str, Any]] = field(default_factory=dict)

	@classmethod
	def load(cls, config: str | pathlib.Path | None = None) -> Self:
		path = cls._resolve(config)
		data = tomllib.loads(path.read_text())
		if path.name == "pyproject.toml":
			data = data.get("tool", {}).get("kuu")
		if isinstance(data, dict) and "default" not in data:
			if any(k in data for k in ("app", "task_modules")):
				data = {"default": data}
		return convert(data, cls, dec_hook=_dec_hook)

	@staticmethod
	def _resolve(config: str | pathlib.Path | None) -> pathlib.Path:
		if config is not None:
			p = pathlib.Path(config)
			if not p.exists():
				raise FileNotFoundError(f"conn_config file not found: {p}")
			return p

		for candidate in ("kuunfig.toml", "pyproject.toml"):
			p = pathlib.Path(candidate)
			if p.exists():
				if candidate == "pyproject.toml":
					raw = tomllib.loads(p.read_text())
					if "kuu" not in raw.get("tool", {}):
						continue
				return p
		raise FileNotFoundError(
			"no kuu conn_config found (looked for ./kuunfig.toml "
			"and [tool.kuu] in ./pyproject.toml)"
		)

	def resolve(self, name: str | None = None) -> Settings:
		if name is None:
			merged = self.default
		else:
			preset = self.presets.get(name)
			if preset is None:
				raise KeyError(f"preset {name!r} not found; available: {sorted(self.presets)}")
			data = _msgspec_to_builtins(self.default, enc_hook=_enc_hook)
			preset_data = {k: v for k, v in preset.items() if v is not None}
			data.update(preset_data)
			merged = convert(data, Settings, dec_hook=_dec_hook)
		if merged.app is None:
			where = f"preset {name!r}" if name else "default"
			raise ValueError(f"{where}: 'app' is required after resolve")
		return merged
