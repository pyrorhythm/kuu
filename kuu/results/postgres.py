from __future__ import annotations

import asyncio
import json
import logging
from asyncio import CancelledError
from typing import TYPE_CHECKING
from weakref import WeakValueDictionary

import anyio

from .._types import _ensure_connected
from ..result import Result
from ..serializers import JSONSerializer
from ..serializers.base import Serializer
from .base import ResultBackend

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
        self._connect_lock = asyncio.Lock()
        self._cleanup_interval = cleanup_interval

        self._sf: WeakValueDictionary[str, anyio.Event] = WeakValueDictionary()
        self._listen_ready = asyncio.Event()
        self._background_tasks: list[asyncio.Task] = []

    async def connect(self) -> None:
        async with self._connect_lock:
            if self._setup_done:
                return

            await self._transport.connect()

            # Execute schema SQL as a single block
            # (splitting on ";" breaks dollar-quoted strings in PL/pgSQL)
            async with self._transport.acq() as conn:
                try:
                    await conn.execute(SCHEMA_SQL)
                except Exception as e:
                    # Ignore "already exists" errors for idempotent operations
                    if "already exists" not in str(e):
                        raise

            # Use asyncio.create_task for background loops to avoid
            # corrupting the anyio cancel scope stack of the calling task.
            self._background_tasks = [
                asyncio.create_task(self._listen_loop()),
                asyncio.create_task(self._cleanup_loop()),
            ]
            # Wait for LISTEN to be established before returning so we
            # don't miss NOTIFYs from results stored immediately after.
            await self._listen_ready.wait()
            self._setup_done = True

    async def close(self) -> None:
        for t in self._background_tasks:
            t.cancel()
        for t in self._background_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._background_tasks.clear()
        self._setup_done = False

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cleanup_interval)
            try:
                async with self._transport.acq() as conn:
                    await conn.execute(
                        "delete from kuu.task_results "
                        "where expires_at is not null and expires_at < now()"
                    )
            except Exception:
                log.warning("event=pg.cleanup_failed", exc_info=True)

    async def _listen_loop(self) -> None:
        log.debug("event=pg.listen_starting")

        def _on_notify(_conn, _pid, _chan, payload) -> None:
            try:
                msg = json.loads(payload)
                key = msg["key"]
                log.debug("event=pg.notify_received key=%s payload=%s", key, payload)
            except (json.JSONDecodeError, KeyError, TypeError):
                log.debug("event=pg.notify_bad_payload payload=%s", payload)
                return
            evt = self._sf.get(key)
            if evt is not None:
                log.debug("event=pg.notify_setting_event key=%s", key)
                # Thread-safe: use call_soon_threadsafe since this runs in asyncpg's thread
                try:
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(evt.set)
                except RuntimeError:
                    # No running loop, fallback to direct set
                    evt.set()
            else:
                log.debug("event=pg.notify_no_event key=%s sf_keys=%s", key, list(self._sf.keys()))

        async with self._transport.acq() as conn:
            await conn.add_listener(_NOTIFY_CHANNEL, _on_notify)
            self._listen_ready.set()
            log.debug("event=pg.listen_registered")
            try:
                # Keep this task alive until cancelled
                await anyio.sleep_forever()
            except CancelledError:
                log.debug("event=pg.listen_cancelled")
            finally:
                if not conn.is_closed():
                    await conn.remove_listener(_NOTIFY_CHANNEL, _on_notify)
        log.debug("event=pg.listen_exited")

    @_ensure_connected
    async def get(
        self, key: str, listen_timeout: float = -1, **kwargs
    ) -> Result | None:
        use_timeout = listen_timeout >= 0
        log.debug("event=pg.get_start key=%s listen_timeout=%s sf_keys=%s", key, listen_timeout, list(self._sf.keys()))

        async with self._transport.acq() as conn:
            row = await conn.fetchrow(
                "select status, value, error, type from kuu.task_results "
                "where task_key = $1 and (expires_at is null or expires_at > now())",
                key,
            )
        if row is not None:
            log.debug("event=pg.get_immediate_hit key=%s", key)
            return Result(
                status=row["status"],
                value=row["value"],
                error=row["error"],
                type=row["type"],
            )

        evt = self._sf.get(key)
        log.debug("event=pg.get_event_check key=%s exists=%s", key, evt is not None)

        if evt is None:
            # we are the first listener; create an event and try to fast-acquire
            evt = anyio.Event()
            self._sf[key] = evt
            log.debug("event=pg.get_event_created key=%s", key)

            async with self._transport.acq() as conn:
                row = await conn.fetchrow(
                    "select status, value, error, type from kuu.task_results "
                    "where task_key = $1 and (expires_at is null or expires_at > now())",
                    key,
                )

            if row is not None:
                log.debug("event=pg.get_fast_acquire key=%s", key)
                evt.set()  # ensure any waiters are woken
                return Result(
                    status=row["status"],
                    value=row["value"],
                    error=row["error"],
                    type=row["type"],
                )

        # else we are second+, event already exists; waiting
        log.debug("event=pg.get_waiting key=%s timeout=%s", key, listen_timeout)

        try:
            with anyio.fail_after(listen_timeout if use_timeout else None):
                await evt.wait()
            log.debug("event=pg.get_event_fired key=%s", key)
        except TimeoutError:
            log.debug("event=pg.get_timeout key=%s", key)
            return None

        async with self._transport.acq() as conn:
            row = await conn.fetchrow(
                "select status, value, error, type from kuu.task_results "
                "where task_key = $1 and (expires_at is null or expires_at > now())",
                key,
            )
        if row is None:
            log.debug("event=pg.get_no_row key=%s", key)
            return None
        log.debug("event=pg.get_returning key=%s", key)
        return Result(
            status=row["status"],
            value=row["value"],
            error=row["error"],
            type=row["type"],
        )

    @_ensure_connected
    async def set(self, key: str, result: Result, ttl: float | None = None) -> None:
        log.debug("event=pg.set_start key=%s status=%s", key, result.status)
        async with self._transport.acq() as conn:
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
            log.debug("event=pg.set_stored key=%s", key)

    @_ensure_connected
    async def set_not_exists(
        self, key: str, result: Result, ttl: float | None = None
    ) -> bool:
        async with self._transport.acq() as conn:
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
            async with self._transport.acq() as conn:
                await conn.execute(
                    "select pg_notify($1, $2)",
                    _NOTIFY_CHANNEL,
                    json.dumps({"key": key, "status": result.status}),
                )

        return inserted
