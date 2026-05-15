from __future__ import annotations

import logging
import re
from typing import Annotated

from typer import Argument, Typer

from kuu._import import import_object, import_tasks
from kuu.app import Kuu

log = logging.getLogger("kuu.cli")


_fqn_re = re.compile(
	r"^[a-zA-Z_][a-zA-Z0-9_]*"
	r"(\.[a-zA-Z_][a-zA-Z0-9_]*)*"
	r":[a-zA-Z_][a-zA-Z0-9_]*$"
)

app = Typer()


def load_app(fqn: str) -> Kuu:
	if not _fqn_re.fullmatch(fqn):
		raise SystemExit(f"invalid app target: {fqn!r}; expected 'module[:attr]'")
	app = import_object(fqn)
	if not isinstance(app, Kuu):
		raise SystemExit(f"{fqn!r} is not a kuu.Q instance")
	return app


@app.command("info")
def info(
	app_spec: Annotated[
		str,
		Argument(help="object specification, e.g. myapp.manager:app"),
	],
	task_modules: Annotated[
		list[str],
		Argument(help="module specifications, which contain tasks to register; e.g. myapp.tasks"),
	],
):
	q = load_app(app_spec)
	import_tasks(task_modules, "", False)
	log.info("event=cli.info.queues queues=%s", sorted(q.registry.queues()))
	for name in sorted(q.registry.names()):
		t = q.registry.get(name)
		if t is None:
			continue
		log.info(
			"event=cli.info.task task=%s queue=%s max_attempts=%s timeout=%s",
			name,
			t.task_queue,
			t.max_attempts,
			t.timeout,
		)
