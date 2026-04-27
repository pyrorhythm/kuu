from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

import anyio

from kuu.config import Kuunfig

if TYPE_CHECKING:
    from kuu.orchestrator.main import Orchestrator

log = getLogger("kuu.orchestrator.dashboard-runner")


class DashboardRunner:
    _orch: Orchestrator
    _config: Kuunfig

    def __init__(self, orch: Orchestrator, config: Kuunfig) -> None:
        self._orch = orch
        self._config = config

    async def run(self, stop_event: anyio.Event) -> None:
        dash_config = self._config.dashboard

        if not dash_config.enable:
            return

        try:
            import uvicorn

            from kuu._import import import_object, import_tasks
            from kuu.web.dashboard import Dashboard
        except ImportError:
            log.exception("dashboard dependencies missing; install kuu[dashboard]")
            return

        kuu = import_object(self._config.app)
        import_tasks(self._config.task_modules, pattern=(), fs_discover=False)
        app = Dashboard(app=kuu, orchestrator=self._orch).build_app()

        if dash_config.path and dash_config.path != "/":
            from starlette.applications import Starlette
            from starlette.routing import Mount

            app = Starlette(routes=[Mount(dash_config.path, app=app)])

        cfg = uvicorn.Config(
            app,
            host=dash_config.host,
            port=dash_config.port,
            log_level="warning",
        )
        server = uvicorn.Server(cfg)
        log.info(
            "dashboard serving on http://%s:%d%s",
            dash_config.host,
            dash_config.port,
            dash_config.path,
        )
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(server.serve)
                await stop_event.wait()
        finally:
            with anyio.move_on_after(delay=self._config.shutdown_timeout):
                await server.shutdown()
            if server.started:
                server.force_exit = True
