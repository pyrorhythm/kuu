from __future__ import annotations

import fnmatch
import re
from logging import getLogger
from pathlib import Path

import anyio
import watchfiles
from watchfiles import Change, DefaultFilter

from kuu._types import _FnAsync
from kuu.config import Kuunfig, WatchSettings

type Changes = set[tuple[Change, str]]
type CallbackFn = _FnAsync[[Changes], None]

log = getLogger("kuu.orchestrator.watcher")


class Watcher:
	_config: Kuunfig
	_callback: CallbackFn

	def __init__(self, config: Kuunfig, callback: CallbackFn) -> None:
		self._config = config
		self._callback = callback

	async def run(self, stop_event: anyio.Event) -> None:
		watch_cfg = self._config.watch
		if not watch_cfg.enable:
			return

		watch_filter = _build_filter(watch_cfg)

		async for changes in watchfiles.awatch(
			watch_cfg.root,
			watch_filter=watch_filter,
			stop_event=stop_event,
			step=int(watch_cfg.reload_delay * 1000),
			debounce=int(watch_cfg.reload_debounce * 1000),
			recursive=True,
		):
			if stop_event.is_set():
				break

			log.info("detected changes in %d file(s)", len(changes))

			await self._callback(changes)


def _build_filter(cfg: WatchSettings) -> _ConfiguredFilter:
	root = cfg.root.resolve()
	patterns: list[str] = [_normalize_glob(str(p)) for p in cfg.exclude]
	if cfg.respect_gitignore:
		patterns.extend(_read_gitignore(root))
	regexes = [re.compile(fnmatch.translate(p)) for p in patterns if p]
	return _ConfiguredFilter(root=root, exclude=regexes)


class _ConfiguredFilter(DefaultFilter):
	"""
	Extends watchfiles' default filter with user-provided globs and gitignore entries

	This class extends watchfiles' default filter (which already drops `.git`; `__pycache__`; editor swap files; etc;) with user-provided globs and gitignore entries; patterns are matched against both absolute and root-relative paths so anchored and unanchored globs both work
	"""

	def __init__(self, root: Path, exclude: list[re.Pattern[str]]) -> None:
		super().__init__()
		self._root = root
		self._exclude = exclude

	def __call__(self, change: Change, path: str) -> bool:
		if not super().__call__(change, path):
			return False
		try:
			rel = str(Path(path).relative_to(self._root))
		except ValueError:
			rel = path
		return not any(rx.match(rel) or rx.match(path) for rx in self._exclude)


def _normalize_glob(pattern: str) -> str:
	pattern = pattern.strip()
	if not pattern:
		return ""
	if pattern.startswith("/"):
		pattern = pattern[1:]
	elif "/" not in pattern.rstrip("/"):
		pattern = "**/" + pattern
	if pattern.endswith("/"):
		pattern += "**"
	return pattern


def _read_gitignore(root: Path) -> list[str]:
	gi = root / ".gitignore"
	if not gi.is_file():
		return []
	out: list[str] = []
	for raw in gi.read_text().splitlines():
		line = raw.strip()
		if not line or line.startswith("#") or line.startswith("!"):
			continue
		out.append(_normalize_glob(line))
	return out
