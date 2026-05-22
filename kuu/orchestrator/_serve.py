from __future__ import annotations

import logging
import os
import shutil
from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer

	from kuu.config import DashboardConfig, MetricsConfig

log = logging.getLogger("kuu.orchestrator.serve")


class MetricsServer:
	def __init__(self) -> None:
		self._metrics_dir: str | None = None
		self._server: WSGIServer | None = None

	async def start(self, cfg: MetricsConfig, *, log_event: str) -> None:
		if not cfg.enable:
			return
		from kuu.prometheus import serve

		self._metrics_dir = await anyio.mkdtemp(prefix="kuu-prom-")
		os.environ["PROMETHEUS_MULTIPROC_DIR"] = self._metrics_dir
		self._server, _ = serve(
			host=cfg.host,
			port=cfg.port,
			multiprocess_dir=self._metrics_dir,
		)
		log.info(
			"%s host=%s port=%d dir=%s",
			log_event,
			cfg.host,
			cfg.port,
			self._metrics_dir,
		)

	def stop(self) -> None:
		srv = self._server
		self._server = None
		if srv is not None:
			try:
				srv.shutdown()
			except Exception as e:
				log.exception("event=metrics_server.shutdown_failed error=%s", e)
		if self._metrics_dir:
			shutil.rmtree(self._metrics_dir, ignore_errors=True)
			self._metrics_dir = None


async def serve_uvicorn_until_stop(
	asgi_app,
	cfg: DashboardConfig,
	stop_event: anyio.Event,
	*,
	shutdown_timeout: float,
	log_event: str,
) -> None:
	import uvicorn

	if cfg.path and cfg.path != "/":
		from starlette.applications import Starlette
		from starlette.routing import Mount

		asgi_app = Starlette(routes=[Mount(cfg.path, app=asgi_app)])

	server_cfg = uvicorn.Config(
		asgi_app,
		host=cfg.host,
		port=cfg.port,
		log_level="warning",
	)
	server = uvicorn.Server(server_cfg)
	log.info("%s host=%s port=%d path=%s", log_event, cfg.host, cfg.port, cfg.path)
	try:
		async with anyio.create_task_group() as tg:
			tg.start_soon(server.serve)
			await stop_event.wait()
			tg.cancel_scope.cancel()
	finally:
		with anyio.move_on_after(delay=shutdown_timeout):
			await server.shutdown()
		if server.started:
			server.force_exit = True
