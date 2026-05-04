from __future__ import annotations

from typing import Annotated

from typer import Option, Typer

app = Typer()


@app.command(
	name="start",
	help="launch kuu's orchestrator instance",
)
def worker(
	config: Annotated[
		str | None,
		Option(
			"--conn_config",
			"-c",
			help="path to conn_config file (TOML). defaults to ./kuunfig.toml or [tool.kuu] in ./pyproject.toml",
		),
	] = None,
	preset: Annotated[
		str | None,
		Option(
			"--preset",
			"-p",
			help="conn_config preset, `default` if unspecified",
		),
	] = None,
	override: Annotated[
		list[str] | None,
		Option(
			"--override",
			"-o",
			help="override a conn_config setting: --override dotted.path=value (repeatable)",
		),
	] = None,
):
	import anyio

	from kuu.config import Kuunfig
	from kuu.orchestrator.main import Orchestrator

	kuucfg = Kuunfig.load(config)
	cfg = kuucfg.resolve(preset)

	if override:
		cfg = cfg.with_overrides(override)

	anyio.run(Orchestrator(cfg).start)
