from __future__ import annotations

from typing import Protocol

from kuu.persistence._rows import LogRow, RunRow


class PersistenceBackend(Protocol):
	"""abstract async backend for persisting task runs and logs"""

	async def connect(self) -> None: ...

	async def close(self) -> None: ...

	async def init_schema(self) -> None: ...

	async def write_runs(self, runs: list[RunRow]) -> None: ...

	async def write_logs(self, logs: list[LogRow]) -> None: ...

	async def query_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		before: float | None = None,
		after: float | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[RunRow]: ...

	async def query_run_attempts(self, message_id: str) -> list[RunRow]: ...

	async def query_logs(
		self, run_id: int, *, limit: int = 500, after_ts: float = 0.0
	) -> list[LogRow]: ...

	async def prune(self, before_ts: float) -> int: ...


class NoopBackend:
	"""backend that does nothing — used when persistence is disabled"""

	async def connect(self) -> None:
		pass

	async def close(self) -> None:
		pass

	async def init_schema(self) -> None:
		pass

	async def write_runs(self, runs: list[RunRow]) -> None:
		pass

	async def write_logs(self, logs: list[LogRow]) -> None:
		pass

	async def query_runs(self, **kwargs) -> list[RunRow]:
		return []

	async def query_run_attempts(self, message_id: str) -> list[RunRow]:
		return []

	async def query_logs(self, run_id: int, **kwargs) -> list[LogRow]:
		return []

	async def prune(self, before_ts: float) -> int:
		return 0
