from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kuu.config import Settings

if TYPE_CHECKING:
	from kuu.app import Kuu

log = logging.getLogger("kuu.orchestrator.app_loader")


class AppLoader:
	def __init__(self, config: Settings) -> None:
		self._config = config
		self._app: Kuu | None = None

	def get(self) -> Kuu | None:
		if self._app is not None:
			return self._app
		try:
			from kuu._import import import_object, import_tasks

			self._app = import_object(self._config.app)  # type: ignore[arg-type]
			import_tasks(self._config.task_modules, pattern=(), fs_discover=False)
		except Exception as e:
			log.exception("event=app_loader.import_failed error=%s", e)
		return self._app
