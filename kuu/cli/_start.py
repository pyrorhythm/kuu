from __future__ import annotations

from typing import Annotated

from typer import Option, Typer

app = Typer()


@app.command(
	name="start",
	help=(
		"launch kuu. without --preset, starts the control plane and forks "
		"one supervisor per preset; with --preset, runs a single leaf "
		"supervisor as before"
	),
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
			help="run only this preset as a single leaf supervisor (legacy mode)",
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

	kuucfg = Kuunfig.load(config)

	if preset is not None:
		from kuu.orchestrator.main import PresetSupervisor

		cfg = kuucfg.resolve(preset)
		if override:
			cfg = cfg.with_overrides(override)
		anyio.run(PresetSupervisor(cfg, preset=preset).start)
		return

	if override:
		kuucfg = _apply_overrides(kuucfg, override)

	from kuu.orchestrator import ControlPlane

	anyio.run(ControlPlane(kuucfg).start)


def _apply_overrides(kuucfg, overrides):
	"""apply CLI overrides to default + every preset uniformly"""
	from kuu.config import Kuunfig

	new_default = kuucfg.default.with_overrides(overrides)
	new_presets = {}
	for name in kuucfg.presets:
		resolved = kuucfg.resolve(name).with_overrides(overrides)
		new_presets[name] = resolved.model_dump()
	data = {"default": new_default.model_dump(), "presets": new_presets}
	return Kuunfig.model_validate(data)
