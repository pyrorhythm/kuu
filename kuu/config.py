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
		description="...",
	)

	respect_gitignore: bool = Field(
		True,
		description="if set, watchfiles would respect .gitignore in addition to [watch.exclude]",
	)

	exclude: list[Path] = Field(
		[Path(".git") / "**"],
		description="globs to exclude from watching",
	)

	reload_delay: float = Field(0.25)

	reload_debounce: float = Field(0.5)


class Kuunfig(BaseModel):
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
		description="count of workers to launch",
	)

	concurrency: PositiveInt = Field(
		64,
		description="how much coroutines at the same time is allowed for single worker",
	)

	prefetch: PositiveInt = Field(
		default_factory=lambda data: max(1, data["concurrency"] // 4),
		description="amount to prefetch (batch) tasks from queue",
	)

	shutdown_timeout: NonNegativeFloat = Field(
		30.0,
		description="max amount of time to wait for graceful workers' shutdown",
	)

	watch_fs: bool = Field(
		False,
		description="watch filesystem changes and reload workers on them",
		deprecated=deprecated("use [watch] enable=true (or WatchKuunfig) instead"),
	)

	metrics: MetricsSettings = Field(MetricsSettings())
	dashboard: WebSettings = Field(WebSettings())
	watch: WatchSettings = Field(WatchSettings())
	scheduler: SchedulerSettings = Field(SchedulerSettings())

	@classmethod
	def load(cls, config: str | pathlib.Path | None = None) -> Self:
		"""
		Load configuration from TOML.

		When `config` is `None`, autodiscover in the current directory:
		prefer `./kuunfig.toml`, then fall back to `[tool.kuu]` in
		`./pyproject.toml`.
		"""
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
