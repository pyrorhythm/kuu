from __future__ import annotations

import json
import pathlib
import re
import tomllib
from pathlib import Path
from typing import Annotated, Any, Self
from warnings import deprecated

import orjson
from pydantic import (
	BaseModel,
	ConfigDict,
	Field,
	NonNegativeFloat,
	PositiveInt,
	StringConstraints,
	create_model,
	model_validator,
)


class SchedulerSettings(BaseModel):
	enable: bool = Field(
		False,
		description=(
			"run the scheduler loop inside the orchestrator. jobs are declared "
			"in code via `app.schedule.cron(...)` / `.every(...)`."
		),
	)


class MetricsSettings(BaseModel):
	enable: bool = Field(False)
	host: str = Field("0.0.0.0")
	port: int = Field(9191)


class WebSettings(BaseModel):
	enable: bool = Field(False)
	host: str = Field("0.0.0.0")
	port: int = Field(8181)
	path: str = Field("/dashboard")


class WatchSettings(BaseModel):
	enable: bool = Field(
		False,
		description="watch filesystem changes and reload workers on every change",
	)

	root: Path = Field(
		default_factory=Path.cwd,
		description="directory to watch for changes",
	)

	respect_gitignore: bool = Field(
		True,
		description="filter .gitignore paths in addition to [watch.exclude] globs",
	)

	exclude: list[Path] = Field(
		[Path(".git") / "**"],
		description="globs to exclude from watching",
	)

	reload_delay: float = Field(0.25)

	reload_debounce: float = Field(0.5)


class Settings(BaseModel):
	"""Flat, resolved application settings used at runtime."""

	model_config = ConfigDict(
		frozen=True,
		extra="forbid",
	)

	app: Annotated[
		str,
		StringConstraints(
			pattern=re.compile(
				r"^[a-zA-Z_][a-zA-Z0-9_]*"
				r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*"
				r":[a-zA-Z_][a-zA-Z0-9_]*$"
			),
		),
	] = Field(
		description="path to the location of `Kuu` instance\nformatted as `dotted.python_module[:instance]`",
		examples=["myapp.kuu.instance:kuu"],
	)

	task_modules: list[
		Annotated[
			str,
			StringConstraints(
				pattern=re.compile(
					r"^[a-zA-Z_][a-zA-Z0-9_]*"
					r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
				),
			),
		]
	] = Field(
		description="modules which declare tasks with this `Kuu` instance\nformatted as `dotted.python_module`",
	)

	queues: list[str] = Field(
		[],
		description=(
			"queues this worker consumes from; empty means auto-discover from "
			"the registered tasks, falling back to the app's default_queue"
		),
	)

	processes: PositiveInt = Field(
		1,
		description="worker subprocesses to spawn",
	)

	concurrency: PositiveInt = Field(
		64,
		description="max concurrent coroutines per worker",
	)

	prefetch: PositiveInt = Field(
		default_factory=lambda data: max(1, data["concurrency"] // 4),
		description="messages to prefetch per broker pull",
	)

	shutdown_timeout: NonNegativeFloat = Field(
		30.0,
		description="seconds to wait for in-flight tasks before forcing shutdown",
	)

	watch_fs: bool = Field(
		False,
		description="reload workers on filesystem changes",
		deprecated=deprecated("use [watch] enable=true (or WatchKuunfig) instead"),
	)

	metrics: MetricsSettings = Field(MetricsSettings())
	dashboard: WebSettings = Field(WebSettings())
	watch: WatchSettings = Field(WatchSettings())
	scheduler: SchedulerSettings = Field(SchedulerSettings())

	def with_overrides(self, overrides: list[str]) -> Self:
		"""
		Return a copy of this config with `dotted.path=value` overrides applied.

		Values are parsed as JSON when possible (so `true`, `42`, `"x"`,
		`[1,2]` all work); otherwise they are kept as raw strings.
		"""
		if not overrides:
			return self
		data: dict[str, Any] = self.model_dump()
		for raw in overrides:
			key, sep, value = raw.partition("=")
			if not sep:
				raise ValueError(f"override must be in form 'dotted.path=value', got: {raw!r}")
			try:
				parsed: Any = orjson.loads(value)
			except json.JSONDecodeError:
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
		return type(self).model_validate(data)


def unset_required(m: type[BaseModel], name: str | None = None) -> type[BaseModel]:
	fields: dict = {
		k: (
			unset_required(v.annotation) if isinstance(v.annotation, BaseModel) else (v.annotation),
			None,
		)
		for k, v in m.model_fields.items()
	}
	return create_model(
		name if name is not None else m.__name__,
		__base__=BaseModel,
		**fields,
	)


SettingsPreset = unset_required(Settings, "SettingsPreset")


class Kuunfig(BaseModel):
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

	model_config = ConfigDict(frozen=True, extra="forbid")

	default: Settings
	presets: dict[str, SettingsPreset] = Field(default_factory=dict)

	@model_validator(mode="before")
	@classmethod
	def _auto_wrap_flat(cls, data: Any) -> Any:
		if isinstance(data, dict) and "default" not in data:
			if any(k in data for k in ("app", "task_modules")):
				data = {"default": data}
		return data

	@classmethod
	def load(cls, config: str | pathlib.Path | None = None) -> Self:
		path = cls._resolve(config)
		data = tomllib.loads(path.read_text())
		if path.name == "pyproject.toml":
			data = data.get("tool", {}).get("kuu", {})
		return cls.model_validate(data)

	@staticmethod
	def _resolve(config: str | pathlib.Path | None) -> pathlib.Path:
		if config is not None:
			p = pathlib.Path(config)
			if not p.exists():
				raise FileNotFoundError(f"config file not found: {p}")
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
			"no kuu config found (looked for ./kuunfig.toml and [tool.kuu] in ./pyproject.toml)"
		)

	def resolve(self, name: str | None = None) -> Settings:
		"""Resolve a named preset by merging it with ``default``.

		Preset values take precedence; ``None`` fields fall back to the
		default.  When *name* is ``None``, returns ``self.default``
		unchanged.
		"""
		if name is None:
			return self.default
		preset = self.presets.get(name)
		if preset is None:
			raise KeyError(f"preset {name!r} not found; available: {sorted(self.presets)}")
		data = self.default.model_dump()
		preset_data = preset.model_dump(exclude_none=True)
		data.update(preset_data)
		return Settings.model_validate(data)
