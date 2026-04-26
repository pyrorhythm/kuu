from __future__ import annotations

import logging
import re
from typing import Annotated

import typer
from typer import Argument, Option

from kuu._import import import_object, import_tasks
from kuu.app import Kuu

log = logging.getLogger("kuu.cli")

app = typer.Typer(name="kuu")

_fqn_re = re.compile(
	r"^[a-zA-Z_][a-zA-Z0-9_]*"
	r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*"
	r":[a-zA-Z_][a-zA-Z0-9_]*$"
)


def _load_app(fqn: str) -> Kuu:
	if not _fqn_re.fullmatch(fqn):
		raise SystemExit(f"invalid app target: {fqn!r}; expected 'module[:attr]'")
	app = import_object(fqn)
	if not isinstance(app, Kuu):
		raise SystemExit(f"{fqn!r} is not a kuu.Q instance")
	return app


@app.command(name="worker")
def _worker(
	app_spec: Annotated[
		str,
		Argument(help="object specification, e.g. myapp.manager:app"),
	],
	task_modules: Annotated[
		list[str],
		Argument(help="module specifications, which contain tasks to register; e.g. myapp.tasks"),
	],
	queues: Annotated[
		list[str] | None,
		Option(
			"--queues",
			"-Q",
			parser=lambda raw: raw.split(",") if raw and isinstance(raw, str) else None,
			help="comma-separated queues (default: all registered)",
		),
	] = None,
	concurrency: Annotated[
		int,
		Option("--concurrency", "-c", help="task concurrency in bounds of one worker"),
	] = 64,
	subprocesses: Annotated[
		int,
		Option(
			"--workers",
			help="number of workers",
		),
	] = 1,
	watch: Annotated[
		bool,
		Option(
			"--watch",
			"-w",
			help="if to watch fs events from cwd",
		),
	] = False,
	prefetch: Annotated[
		int | None,
		Option(
			help="how much messages do we need to prefetch (batching); default = max(1, concurrency // 4)"
		),
	] = None,
	shutdown_timeout: Annotated[
		float,
		Option(),
	] = 30.0,
	metrics_port: Annotated[
		int | None,
		Option(
			"--metrics-port",
			help="if set, orchestrator serves aggregated /metrics on this port",
		),
	] = None,
	metrics_host: Annotated[
		str,
		Option("--metrics-host", help="bind address for the metrics server"),
	] = "0.0.0.0",
):
	if prefetch is None:
		prefetch = max(1, concurrency // 4)

	import anyio

	from kuu.orchestrator.main import Orchestrator

	orch = Orchestrator(
		app_spec=app_spec,
		task_modules=task_modules,
		queues=queues,
		concurrency=concurrency,
		prefetch=prefetch,
		shutdown_timeout=shutdown_timeout,
		subprocesses=subprocesses,
		watch_fs=watch,
		metrics_port=metrics_port,
		metrics_host=metrics_host,
	)
	anyio.run(orch.start)


@app.command("info")
def _info(
	app_spec: Annotated[
		str,
		Argument(help="object specification, e.g. myapp.manager:app"),
	],
	task_modules: Annotated[
		list[str],
		Argument(help="module specifications, which contain tasks to register; e.g. myapp.tasks"),
	],
):
	q = _load_app(app_spec)
	import_tasks(task_modules, "", False)
	log.info(f"queues: {sorted(q.registry.queues())}")
	for name in sorted(q.registry.names()):
		t = q.registry.get(name)
		if t is None:
			continue
		log.info(
			"%s\tqueue=%s max_attempts=%s timeout=%s",
			name,
			t.task_queue,
			t.max_attempts,
			t.timeout,
		)
