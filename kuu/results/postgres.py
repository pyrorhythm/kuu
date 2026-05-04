from __future__ import annotations

import asyncio
import json
import logging
from asyncio import CancelledError
from typing import Any, TYPE_CHECKING
from weakref import WeakValueDictionary

import anyio
from anyio.abc import TaskGroup

from .base import ResultBackend
from .._types import _ensure_connected
from ..result import Result
from ..serializers import JSONSerializer
from ..serializers.base import Serializer

if TYPE_CHECKING:
	from ..transports.postgres._transport import PostgresTransport

log = logging.getLogger("kuu.results.postgres")

_NOTIFY_CHANNEL = "kuu_task_result"

SCHEMA_SQL = """
             create schema if not exists kuu;

             create table if not exists kuu.task_results
             (
                 task_key   text primary key,
                 status     text        not null,
                 value      bytea,
                 error      text,
                 type       text,
                 created_at timestamptz not null default now(),
                 expires_at timestamptz
             );

             create index if not exists idx_task_results_expires
                 on kuu.task_results (expires_at)
                 where expires_at is not null;

             create or replace function kuu.notify_task_result()
                 returns trigger as
             $$
             begin
                 perform pg_notify(
                         'kuu_task_result',
                         json_build_object('key', new.task_key, 'status', new.status)::text
                         );
                 return new;
             end;
             $$ language plpgsql;

             do
             $$
                 begin
                     if not exists (select 1
                                    from pg_trigger
                                    where tgname = 'task_result_notify'
                                      and tgrelid = 'kuu.task_results'::regclass) then
                         create trigger task_result_notify
                             after insert or update
                             on kuu.task_results
                             for each row
                         execute function kuu.notify_task_result();
                     end if;
                 end
             $$; \
             """


class PostgresResults(ResultBackend):
	def __init__(
		self,
		transport: PostgresTransport,
		*,
		serializer: Serializer = JSONSerializer(),
		marshal_types: bool = True,
		ttl: float | None = 86400,
		replay: bool = True,
		store_errors: bool = True,
		cleanup_interval: float = 300.0,
	) -> None:
		self.serializer = serializer
		self.marshal_types = marshal_types
		self.ttl = ttl
		self.replay = replay
		self.store_errors = store_errors
		self._transport = transport
		self._setup_done = False
		self._cleanup_interval = cleanup_interval

		self._sf: WeakValueDictionary[str, anyio.Event] = WeakValueDictionary()
		self._tg_background: TaskGroup

	def _acq(self) -> Any:
		return self._transport.acq()

	async def connect(self) -> None:
		if self._setup_done:
			return

		async with self._acq() as conn:
			await conn.execute(SCHEMA_SQL)

		self._tg_background = await anyio.create_task_group().__aenter__()
		self._tg_background.start_soon(self._listen_loop)
		self._tg_background.start_soon(self._cleanup_loop)
		self._setup_done = True

	async def close(self) -> None:
		await self._tg_background.__aexit__(CancelledError, None, None)
		await self._transport.close()
		self._setup_done = False

	async def _cleanup_loop(self) -> None:
		while True:
			await asyncio.sleep(self._cleanup_interval)
			try:
				async with self._acq() as conn:
					await conn.execute(
						"delete from kuu.task_results "
						"where expires_at is not null and expires_at < now()"
					)
			except Exception:
				log.warning("pg cleanup tick failed", exc_info=True)

	async def _listen_loop(self) -> None:
		def _on_notify(_conn, _pid, _chan, payload) -> None:
			try:
				msg = json.loads(payload)
				key = msg["key"]
			except (json.JSONDecodeError, KeyError, TypeError):
				log.debug("bad NOTIFY payload: %s", payload)
				return
			evt = self._sf.get(key)
			if evt is not None:
				evt.set()

		async with self._acq() as conn:
			await conn.add_listener(_NOTIFY_CHANNEL, _on_notify)
			try:
				await anyio.sleep_forever()
			except CancelledError:
				await conn.remove_listener(_NOTIFY_CHANNEL, _on_notify)

	@_ensure_connected
	async def get(self, key: str, listen_timeout: float = -1, **kwargs) -> Result | None:
		use_timeout = listen_timeout >= 0

		async with self._acq() as conn:
			row = await conn.fetchrow(
				"select status, value, error, type from kuu.task_results "
				"where task_key = $1 and (expires_at is null or expires_at > now())",
				key,
			)
		if row is not None:
			return Result(
				status=row["status"],
				value=bytes(row["value"]) or None,
				error=row["error"],
				type=row["type"],
			)

		evt = self._sf.get(key)

		if evt is None:
			# we are the first listener; create an event and try to fast-acquire

			evt = anyio.Event()
			self._sf[key] = evt

			async with self._acq() as conn:
				row = await conn.fetchrow(
					"select status, value, error, type from kuu.task_results "
					"where task_key = $1 and (expires_at is null or expires_at > now())",
					key,
				)

			if row is not None:
				return Result(
					status=row["status"],
					value=bytes(row["value"]) or None,
					error=row["error"],
					type=row["type"],
				)

		# else we are second+, event already exists; waiting

		try:
			with anyio.fail_after(listen_timeout if use_timeout else None):
				await evt.wait()
		except TimeoutError:
			return None

		async with self._acq() as conn:
			row = await conn.fetchrow(
				"select status, value, error, type from kuu.task_results "
				"where task_key = $1 and (expires_at is null or expires_at > now())",
				key,
			)
		if row is None:
			return None
		return Result(
			status=row["status"],
			value=bytes(row["value"]) or None,
			error=row["error"],
			type=row["type"],
		)

	@_ensure_connected
	async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
		async with self._acq() as conn:
			await conn.execute(
				"insert into kuu.task_results "
				"(task_key, status, value, error, type, expires_at) "
				"values ($1, $2, $3, $4, $5, now() + make_interval(secs := $6)) "
				"on conflict (task_key) do update set status = excluded.status,"
				"value = excluded.value, error = excluded.error,"
				"type = excluded.type, expires_at = excluded.expires_at",
				key,
				result.status,
				result.value,
				result.error,
				result.type,
				ttl or self.ttl,
			)

	@_ensure_connected
	async def set_not_exists(self, key: str, result: Result, ttl: float | None = None) -> bool:
		async with self._acq() as conn:
			status = await conn.execute(
				"insert into kuu.task_results "
				"(task_key, status, value, error, type, expires_at) "
				"values ($1, $2, $3, $4, $5, now() + make_interval(secs => $6)) "
				"on conflict do nothing",
				key,
				result.status,
				result.value,
				result.error,
				result.type,
				ttl or self.ttl,
			)

		inserted = status is not None and status.endswith(" 1")
		if not inserted:
			async with self._acq() as conn:
				await conn.execute(
					"select pg_notify($1, $2)",
					_NOTIFY_CHANNEL,
					json.dumps({"key": key, "status": result.status}),
				)

		return inserted
