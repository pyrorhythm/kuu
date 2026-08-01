from __future__ import annotations

from datetime import datetime
from typing import Protocol

from kuu.persistence._rows import LogRow, LogicalRunRow, RunRow


class PersistenceBackend(Protocol):
	"""abstract async backend for persisting task runs and logs"""

	async def connect(self) -> None: ...

	async def close(self) -> None: ...

	async def init_schema(self) -> None: ...

	async def write_runs(self, runs: list[RunRow]) -> None: ...

	async def write_logical_runs(self, runs: list[LogicalRunRow]) -> None: ...

	async def get_logical_run(self, message_id: str) -> LogicalRunRow | None: ...

	async def query_logical_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		before: datetime | None = None,
		after: datetime | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[LogicalRunRow]: ...

	async def query_attempts_for_runs(self, message_ids: list[str]) -> list[RunRow]: ...

	async def mark_cancel_requested(self, message_id: str, at: datetime) -> bool: ...

	async def mark_instance_unknown(self, instance_id: str, at: datetime) -> int: ...

	async def mark_previous_attempts_lost(self, message_id: str, attempt: int) -> int: ...

	async def write_logs(self, logs: list[LogRow]) -> None: ...

	async def query_runs(
		self,
		*,
		task: str | None = None,
		status: str | None = None,
		before: datetime | None = None,
		after: datetime | None = None,
		limit: int = 100,
		offset: int = 0,
	) -> list[RunRow]: ...

	async def query_run_attempts(self, message_id: str) -> list[RunRow]: ...

	async def query_logs(
		self,
		message_id: str,
		attempt: int,
		*,
		after_id: int | None = None,
		after_dt: datetime | None = None,
		limit: int = 500,
	) -> list[LogRow]: ...

	async def prune(self, before_ts: datetime, max_runs: int | None = None) -> int: ...


class NoopBackend:
	"""backend that does nothing - used when persistence is disabled"""

	async def connect(self) -> None:
		pass

	async def close(self) -> None:
		pass

	async def init_schema(self) -> None:
		pass

	async def write_runs(self, runs: list[RunRow]) -> None:
		pass

	async def write_logical_runs(self, runs: list[LogicalRunRow]) -> None:
		pass

	async def get_logical_run(self, message_id: str) -> LogicalRunRow | None:
		return None

	async def query_logical_runs(self, **kwargs) -> list[LogicalRunRow]:
		return []

	async def query_attempts_for_runs(self, message_ids: list[str]) -> list[RunRow]:
		return []

	async def mark_cancel_requested(self, message_id: str, at: datetime) -> bool:
		return False

	async def mark_instance_unknown(self, instance_id: str, at: datetime) -> int:
		return 0

	async def mark_previous_attempts_lost(self, message_id: str, attempt: int) -> int:
		return 0

	async def write_logs(self, logs: list[LogRow]) -> None:
		pass

	async def query_runs(self, **kwargs) -> list[RunRow]:
		return []

	async def query_run_attempts(self, message_id: str) -> list[RunRow]:
		return []

	async def query_logs(self, message_id: str, attempt: int, **kwargs) -> list[LogRow]:
		return []

	async def prune(self, before_ts: datetime, max_runs: int | None = None) -> int:
		return 0
