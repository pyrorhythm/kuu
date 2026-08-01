from __future__ import annotations

from typing import Annotated

from typer import Option, Typer

app = Typer()


@app.command(
	name="dashboard",
	help=(
		"run the standalone dashboard collector: serves the UI and accepts "
		"observability uplinks from remote leaf processes via /_ingest"
	),
)
def dashboard(
	host: Annotated[str, Option("--host", help="bind host")] = "127.0.0.1",
	port: Annotated[int, Option("--port", help="bind port")] = 8181,
	path: Annotated[str, Option("--path", help="mount path (e.g. /dashboard)")] = "/",
	live_only: Annotated[
		bool, Option("--live-only", help="disable persistence, history, Retry, and Replay")
	] = False,
):
	import anyio

	anyio.run(_serve_collector, host, port, path, live_only)


async def _serve_collector(host: str, port: int, path: str, live_only: bool) -> None:
	import logging

	import anyio
	import os as _os

	import uvicorn
	from starlette.applications import Starlette
	from starlette.routing import Mount

	from kuu.config import PersistenceConfig
	from kuu.observability import InMemoryRegistry
	from kuu.persistence import PersistenceWorker, create_backend
	from kuu.web.dashboard import Dashboard

	log = logging.getLogger("kuu.cli.dashboard")

	registry = InMemoryRegistry()
	persistence_cfg = PersistenceConfig(enable=not live_only)
	persistence_worker = None
	if persistence_cfg.enable:
		persistence_worker = PersistenceWorker(create_backend(persistence_cfg), persistence_cfg)
	dash = Dashboard(
		registry=registry,
		ingest_token=_os.environ.get("KUU_DASHBOARD_TOKEN"),
		persistence_backend=(
			persistence_worker.backend if persistence_worker is not None else None
		),
		persistence_worker=persistence_worker,
		trace_url_template=persistence_cfg.trace_url_template,
	)
	asgi = dash.build_app()
	if path and path != "/":
		asgi = Starlette(routes=[Mount(path, app=asgi)])

	cfg = uvicorn.Config(asgi, host=host, port=port, log_level="warning")
	server = uvicorn.Server(cfg)
	if host not in {"127.0.0.1", "localhost", "::1"}:
		log.warning(
			"event=cli.dashboard.exposed host=%s warning=%s",
			host,
			"no built-in authentication; use an authenticated reverse proxy",
		)
	log.info(
		"event=cli.dashboard.serving host=%s port=%d path=%s ingest_path=%s live_only=%s",
		host,
		port,
		path,
		path if path.endswith("/") else path + "/",
		live_only,
	)
	if persistence_worker is None:
		await server.serve()
		return
	stop = anyio.Event()
	async with anyio.create_task_group() as tg:
		tg.start_soon(persistence_worker.run, stop)
		await persistence_worker.ready.wait()
		try:
			await server.serve()
		finally:
			stop.set()
