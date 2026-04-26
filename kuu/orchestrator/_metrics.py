import os
import shutil
from logging import getLogger
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread

if TYPE_CHECKING:
	from wsgiref.simple_server import WSGIServer

	from kuu.config import Kuunfig

log = getLogger("kuu.orchestrator.metrics-runner")


class MetricsRunner:
	_config: Kuunfig
	_server: WSGIServer | None

	def __init__(self, config: Kuunfig) -> None:
		self._config = config

	async def _start_metrics_server(self) -> None:
		metrics_config = self._config.metrics
		if not metrics_config.enable:
			return

		from kuu.prometheus import serve

		os.environ["PROMETHEUS_MULTIPROC_DIR"] = await anyio.mkdtemp(prefix="kuu-prom-")

		self._server, _ = serve(
			host=metrics_config.host,
			port=metrics_config.port,
		)
		log.info(
			"prometheus aggregator on %s:%d",
			metrics_config.host,
			metrics_config.port,
		)

	async def _stop_metrics_server(self) -> None:
		srv = self._server
		self._server = None
		if srv is not None:
			try:
				await anyio.to_thread.run_sync(srv.shutdown)
			except Exception:
				log.exception("failed to shut down metrics server")
		if _tmpdir := await anyio.gettempdir():
			shutil.rmtree(_tmpdir, ignore_errors=True)
