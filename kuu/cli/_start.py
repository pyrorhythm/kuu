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
			"--config",
			"-c",
			help="path to config file (TOML). defaults to ./kuunfig.toml or [tool.kuu] in ./pyproject.toml",
		),
	] = None,
	override: Annotated[
		list[str] | None,
		Option(
			"--override",
			"-o",
			help="override a config setting: --override dotted.path=value (repeatable)",
		),
	] = None,
):
	import anyio

	from kuu.config import Kuunfig
	from kuu.orchestrator.main import Orchestrator

	cfg = Kuunfig.load(config)
	if override:
		cfg = cfg.with_overrides(override)

	anyio.run(Orchestrator(cfg).start)
