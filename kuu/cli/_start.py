from __future__ import annotations

import os
from typing import Annotated

from typer import Option, Typer

app = Typer()


@app.command(
	name="start",
	help=(
		"launch kuu. without --preset, starts the control plane and forks "
		"one supervisor per preset; with --preset, runs a single leaf "
		"supervisor (legacy embedded dashboard, or remote uplink if "
		"--uplink/$KUU_DASHBOARD_URL is set)"
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
			help="run only this preset as a single leaf supervisor",
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
	uplink: Annotated[
		str | None,
		Option(
			"--uplink",
			help="ws url of a remote dashboard collector; falls back to $KUU_DASHBOARD_URL",
		),
	] = None,
):
	import anyio

	from kuu.config import Kuunfig

	kuucfg = Kuunfig.load(config)

	if preset is not None:
		cfg = kuucfg.resolve(preset)
		if override:
			cfg = cfg.with_overrides(override)
		uplink_url = uplink or os.environ.get("KUU_DASHBOARD_URL")
		anyio.run(_run_leaf, cfg, preset, uplink_url)
		return

	if override:
		kuucfg = _apply_overrides(kuucfg, override)

	from kuu.orchestrator import ControlPlane

	anyio.run(ControlPlane(kuucfg).start)


async def _run_leaf(cfg, preset: str, uplink_url: str | None) -> None:
	from kuu.orchestrator.main import PresetSupervisor

	if not uplink_url:
		await PresetSupervisor(cfg, preset=preset).start()
		return

	import anyio as _anyio

	from kuu.observability import WsUplink

	uplink = WsUplink(uplink_url)
	sup = PresetSupervisor(cfg, preset=preset, events_sink=uplink.sink)
	async with _anyio.create_task_group() as tg:
		tg.start_soon(uplink.run, sup._stop_event)
		tg.start_soon(sup.start)


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
