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
	host: Annotated[str, Option("--host", help="bind host")] = "0.0.0.0",
	port: Annotated[int, Option("--port", help="bind port")] = 8181,
	path: Annotated[str, Option("--path", help="mount path (e.g. /dashboard)")] = "/",
):
	import anyio

	anyio.run(_serve_collector, host, port, path)


async def _serve_collector(host: str, port: int, path: str) -> None:
	import logging
	import os as _os

	import uvicorn
	from starlette.applications import Starlette
	from starlette.routing import Mount

	from kuu.observability import InMemoryRegistry
	from kuu.web.dashboard import Dashboard

	log = logging.getLogger("kuu.cli.dashboard")

	registry = InMemoryRegistry()
	dash = Dashboard(
		registry=registry,
		ingest_token=_os.environ.get("KUU_DASHBOARD_TOKEN"),
	)
	asgi = dash.build_app()
	if path and path != "/":
		asgi = Starlette(routes=[Mount(path, app=asgi)])

	cfg = uvicorn.Config(asgi, host=host, port=port, log_level="warning")
	server = uvicorn.Server(cfg)
	log.info(
		"dashboard collector serving on http://%s:%d%s (ws ingest at %s_ingest)",
		host,
		port,
		path,
		path if path.endswith("/") else path + "/",
	)
	await server.serve()
