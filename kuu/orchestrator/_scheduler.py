from __future__ import annotations

from logging import getLogger

import anyio

from kuu._import import import_object, import_tasks
from kuu.config import Settings

log = getLogger("kuu.orchestrator.scheduler-runner")


class SchedulerRunner:
	_config: Settings

	def __init__(self, config: Settings) -> None:
		self._config = config

	async def run(self, stop_event: anyio.Event) -> None:
		if not self._config.scheduler.enable:
			return

		app = import_object(self._config.app)
		import_tasks(self._config.task_modules, "", False)

		scheduler = app.schedule
		log.info("scheduler running with %d job(s)", len(scheduler.jobs))

		async def _run() -> None:
			await scheduler.run(install_signals=False)

		async with anyio.create_task_group() as tg:
			tg.start_soon(_run)
			await stop_event.wait()
			tg.cancel_scope.cancel()
