from __future__ import annotations

import logging

import anyio

from kuu._import import import_object, import_tasks
from kuu.worker import Worker

log = logging.getLogger("kuu.orchestrator.worker_proc")


def run_worker(
	app_spec: str,
	task_modules: list[str],
	queues: list[str] | None,
	concurrency: int,
	prefetch: int,
	shutdown_timeout: float,
) -> None:
	log.info("worker process starting")
	q = import_object(app_spec)
	import_tasks(task_modules, "", False)

	anyio.run(
		Worker(
			app=q,
			queues=queues,
			concurrency=concurrency,
			prefetch=prefetch,
			shutdown_timeout=shutdown_timeout,
		).run
	)
	log.info("worker process exiting")
