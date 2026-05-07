Usage
=====

A tour of Kuu's features, ordered by how you'd reach for them.

Defining the App
----------------

.. code-block:: python

   from kuu import Kuu
   from kuu.brokers.redis import RedisBroker
   from kuu.results.redis import RedisResults

   app = Kuu(
       broker=RedisBroker(url="redis://localhost:6379/0"),
       default_queue="default",
       results=RedisResults(url="redis://localhost:6379/0"),
   )

``Kuu`` owns transport (``broker``), result storage (``results``), middleware,
the task registry and the event bus.

Tasks
-----

Declaring
~~~~~~~~~

.. code-block:: python

   @app.task
   async def charge(user_id: int, amount_cents: int) -> dict:
       return {"ok": True, "charged": amount_cents}

The parametrized form adds more options:

.. code-block:: python

   @app.task(queue="payments", max_attempts=3, timeout=10.0)
   async def charge(user_id: int, amount_cents: int) -> dict: ...

Sync Functions
~~~~~~~~~~~~~~

Sync functions work too. Set ``blocking=True`` to run them in a worker thread:

.. code-block:: python

   @app.task(blocking=True)
   def render_pdf(invoice_id: int) -> bytes: ...

Combining ``blocking=True`` with an ``async def`` raises ``TypeError``. Async
functions never need it.

Calling
~~~~~~~

Three ways:

.. code-block:: python

   # 1) in-process direct call (no broker hop)
   result = await charge(user_id=1, amount_cents=500)

   # 2) enqueue and forget
   handle = await charge.q(user_id=1, amount_cents=500)

   # 3) enqueue and wait for the result
   result = await handle.result(timeout=30)

``charge.q(...)`` returns a ``TaskHandle[ChargeResult]`` typed off the
function's return annotation, so ``result`` keeps the right type without
casts.

``handle.result()`` polls the result backend every 0.2 s by default. Pass
``poll=`` to change the interval:

.. code-block:: python

   result = await handle.result(timeout=30, poll=0.5)

Delayed Enqueueing
~~~~~~~~~~~~~~~~~~

Pass ``not_before`` to ``enqueue_by_name`` to schedule future delivery.
The broker holds the message until that UTC datetime, then delivers it:

.. code-block:: python

   from datetime import datetime, timedelta, timezone
   from kuu.message import Payload

   run_at = utcnow() + timedelta(minutes=10)

   await app.enqueue_by_name(
       "myapp.tasks:charge",
       Payload(args=(1, 500)),
       not_before=run_at,
   )

For Redis, delayed messages live in a sorted set and are pumped into the live
stream when due. For NATS and Memory brokers, an in-process loop handles
promotion.

Idempotency
~~~~~~~~~~~

Pass ``idempotency_key`` in headers to deduplicate against the result backend:

.. code-block:: python

   await app.enqueue_by_name(
       "myapp.tasks:charge",
       Payload(args=(1, 500)),
       headers={"idempotency_key": "charge:order-42"},
   )

If a result already exists at that key, the worker ack's and skips execution.

Error Control
-------------

Tasks signal their own outcome by raising one of two sentinel exceptions.
Any other uncaught exception is treated as a failure.

Retry
~~~~~

Raise ``RetryErr`` to re-queue the message (immediately or after a delay)
without consuming an attempt:

.. code-block:: python

   from kuu import RetryErr

   @app.task
   async def sync_inventory(sku: str) -> None:
       if not upstream_available():
           raise RetryErr(delay=30.0)  # retry in 30 s

``delay`` is in seconds. Omit it to let ``RetryMiddleware`` compute the
backoff. The worker increments ``message.attempt`` on every retry; once
``max_attempts`` is reached the task is declared dead regardless of how
``RetryErr`` was raised.

Reject
~~~~~~

Raise ``RejectErr`` to discard the message without retrying. Pass
``requeue=True`` to put it back on the queue:

.. code-block:: python

   from kuu import RejectErr

   @app.task
   async def process_webhook(body: dict) -> None:
       if body.get("version") != 2:
           raise RejectErr("unsupported webhook version")  # drop silently

Brokers
-------

Three brokers are bundled, all implementing the same ``Broker`` Protocol.

Redis Streams
~~~~~~~~~~~~~

.. code-block:: python

   from kuu.brokers.redis import RedisBroker

   broker = RedisBroker(
       url="redis://localhost:6379/0",
       group="payments",
       consumer="payments-1",
   )

Live messages flow through Streams; scheduled (``not_before``) messages live
in a sorted set and are pumped into the live stream when due. Stale pending
entries from dead consumers are reclaimed automatically.

Redis Cluster and Sentinel
^^^^^^^^^^^^^^^^^^^^^^^^^^

The convenience ``url=`` argument targets a single node. For cluster or
sentinel deployments, build a ``RedisTransport`` directly and pass it via
``transport=``:

.. code-block:: python

   from kuu import ClusterConfig, SentinelConfig, RedisTransport
   from kuu.brokers.redis import RedisBroker
   from kuu.results.redis import RedisResults

   # Redis Cluster
   broker = RedisBroker(
       transport=RedisTransport(ClusterConfig(
           url="redis://node1:6379",
           read_from_replicas=True,
       ))
   )

   # Redis Sentinel
   broker = RedisBroker(
       transport=RedisTransport(SentinelConfig(
           hosts=(("sentinel1", 26379), ("sentinel2", 26379)),
           service_name="mymaster",
       ))
   )

``RedisResults`` accepts the same ``transport=`` parameter, so a single
``RedisTransport`` can back both the broker and the result store.

.. code-block:: python

   transport = RedisTransport(ClusterConfig(url="redis://node1:6379"))
   broker  = RedisBroker(transport=transport)
   results = RedisResults(transport=transport)

In cluster mode the broker hash-tags stream and sorted-set keys with
``{queue}`` so all keys for a given queue land on the same slot.

NATS JetStream
~~~~~~~~~~~~~~

.. code-block:: python

   from kuu.brokers.nats import NatsBroker

   broker = NatsBroker(
       servers="nats://localhost:4222",
       stream="kuu",
   )

In-Memory
~~~~~~~~~

For tests and single-process setups:

.. code-block:: python

   from kuu.brokers.memory import MemoryBroker

   broker = MemoryBroker(buffer=1024)

Result Backend
--------------

Results are optional. When set, task return values persist under the message
id (or ``idempotency_key`` if present) and ``TaskHandle.result()`` fetches
them.

.. code-block:: python

   from kuu.results.redis import RedisResults

   results = RedisResults(
       url="redis://localhost:6379/0",
       prefix="kuu:r:",
       ttl=86400,  # entries expire after a day; None means no expiry
       replay=True,  # short-circuit on cache hit
       store_errors=True,  # persist terminal failures so callers can observe them
   )

``replay=True`` enables idempotency_key-based deduplication: the worker
checks the backend before running and skips execution if a result already
exists.

Middleware
----------

Three are bundled, same Protocol as user middleware:

.. code-block:: python

   from kuu.middleware import LoggingMiddleware, RetryMiddleware, TimeoutMiddleware

   app = Kuu(
       broker=...,
       middleware=[
           LoggingMiddleware(),
           RetryMiddleware(base=0.5, cap=60.0, jitter=0.2),
           TimeoutMiddleware(seconds=30.0),
       ],
   )

- ``LoggingMiddleware``: one line on start, one on success/failure with
  duration.
- ``RetryMiddleware``: catches ``RetryErr`` and schedules the next attempt
  with exponential backoff plus jitter. Pass ``retry_on`` to also auto-retry
  on specific exception types:

  .. code-block:: python

     RetryMiddleware(base=0.5, cap=60.0, jitter=0.2, retry_on=(httpx.HTTPError,))

  Backoff: ``min(cap, base * 2 ** attempt)`` with jitter applied as a
  fractional multiplier in ``[-jitter, +jitter]``.
- ``TimeoutMiddleware``: per-task ``Task.timeout`` wins; this ``seconds``
  is the default.

Custom Middleware
~~~~~~~~~~~~~~~~~

.. code-block:: python

   class HeaderInjector:
       async def __call__(self, ctx, call_next):
           if ctx.phase == "enqueue":
               ctx.message.headers["x-tenant"] = current_tenant()
           return await call_next()

``ctx.phase`` is ``"enqueue"`` for the publish path and ``"process"`` for
the worker path. Most middleware filters on phase.

Scheduler
---------

Jobs are declared via ``app.every()`` or ``app.sched()`` decorators, or
programmatically through the schedule object. The scheduler loop runs in
the orchestrator process when ``[scheduler] enable = true``.

Interval jobs
~~~~~~~~~~~~~

.. code-block:: python

   from datetime import timedelta

   @app.task
   async def refresh_balance() -> None: ...

   # Decorator style
   @app.every(timedelta(minutes=5))
   async def refresh() -> None: ...

   # Programmatic style
   app.schedule.add_every(timedelta(minutes=5), "myapp.tasks:refresh")

Optional kwargs: ``id``, ``queue``, ``headers``, ``max_attempts``.

Schedule-based jobs (cron-like)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Schedule jobs use composable schedule objects instead of cron expressions:

.. code-block:: python

   from kuu.scheduler.schedule import at, every, on, on_day, in_month, between, Mon, Wed, Fri

   # Every weekday at 09:00
   @app.sched(on(Mon, Tue, Wed, Thu, Fri) & at(time(9, 0)))
   async def morning_report() -> None: ...

   # Every 4 hours, anchored from midnight
   @app.sched(every(hours=4, starting=time(0, 0)))
   async def periodic_sync() -> None: ...

   # 1st and 15th of each month at 03:00
   @app.sched(on_day(1, 15) & at(time(3, 0)))
   async def billing() -> None: ...

   # Between 09:00 and 17:00, every 30 minutes
   @app.sched(every(minutes=30) & between(time(9), time(17)))
   async def office_hours_job() -> None: ...

Schedule objects compose with ``&`` (AND — all must match) and ``|`` (OR —
any matches):

.. list-table::
   :header-rows: 1

   * - Schedule
     - Purpose
   * - ``at(t)``
     - Daily at wall-clock time ``t``
   * - ``every(*, hours, minutes, seconds, starting)``
     - Fixed interval within each day
   * - ``on(*weekdays)``
     - Restrict to specific weekdays (Mon–Sun)
   * - ``on_day(*days)``
     - Restrict to days of month (1–31)
   * - ``in_month(*months)``
     - Restrict to specific months (Jan–Dec)
   * - ``between(start, end)``
     - Restrict to time window within each day

Programmatic registration:

.. code-block:: python

   from kuu.scheduler.schedule import at, on, Mon, Wed, Fri

   app.schedule.add_schedule(at(time(9, 0)) & on(Mon, Wed, Fri), "myapp.tasks:report")

Serializers
-----------

Brokers and result backends use a ``Serializer`` to marshal messages. The
default is ``JSONSerializer``, backed by ``msgspec.json``. Custom per-type
encoders/decoders can be registered via :class:`kuu.marshal.Marshal`.

Msgpack
~~~~~~~

Faster and more compact than JSON for binary payloads. Requires the
``msgspec`` extra:

.. code-block:: shell

   uv add "kuu[msgspec]"

.. code-block:: python

   from kuu.serializers import MsgpackSerializer
   from kuu.brokers.redis import RedisBroker
   from kuu.results.redis import RedisResults

   broker  = RedisBroker(url=..., serializer=MsgpackSerializer())
   results = RedisResults(url=..., serializer=MsgpackSerializer())

Pickle
~~~~~~

Supports arbitrary Python objects but **executes arbitrary code on
deserialization**. Only use in trusted environments.

Set ``KUU_ALLOW_PICKLE=yes`` in the environment to suppress the per-call
``SecurityWarning``:

.. code-block:: python

   from kuu.serializers import PickleSerializer

   broker = RedisBroker(url=..., serializer=PickleSerializer())

Custom Serializer
~~~~~~~~~~~~~~~~~

Implement the ``Serializer`` protocol:

.. code-block:: python

   from kuu import Serializer

   class MySerializer:
       def marshal(self, data) -> bytes: ...
       def unmarshal(self, data: bytes, into=None): ...

Prometheus Metrics
------------------

Two integration points: a worker-side emitter and client-side middleware.
Both wire onto ``app.events``.

Worker Metrics
~~~~~~~~~~~~~~

When ``[metrics] enable = true``, the orchestrator runs an aggregator HTTP
server on ``metrics.port`` and tells each worker subprocess to write to a
shared multiprocess directory.

.. code-block:: toml

   [metrics]
   enable = true
   host = "0.0.0.0"
   port = 9191

Hit ``http://host:9191/metrics``.

Client-Side
~~~~~~~~~~~

.. code-block:: python

   from kuu.prometheus import ClientMetrics

   app = Kuu(broker=..., middleware=[ClientMetrics()])

This records ``client_enqueued_total``, ``client_enqueue_errors_total`` and
``client_enqueue_duration_seconds`` from every ``task.q(...)`` call, on the
same registry the worker side uses.

To expose them from your own ASGI app:

.. code-block:: python

   from fastapi import FastAPI
   from kuu.prometheus import asgi_app

   api = FastAPI()
   api.mount("/metrics", asgi_app())

Dashboard
---------

Requires the ``dashboard`` extra. Shows registered tasks, scheduled jobs,
broker depth and live worker count.

.. code-block:: toml

   [dashboard]
   enable = true
   host = "0.0.0.0"
   port = 8181
   path = "/dashboard"

The orchestrator imports the Kuu app via ``config.app``, mounts the dashboard
under ``path`` and serves it via uvicorn alongside the worker pool.

Events
------

Every ``Kuu`` instance carries an ``app.events`` object with named signals.
Connect a handler to observe lifecycle events:

.. code-block:: python

   @app.events.task_succeeded.connect
   async def on_success(msg, elapsed: float) -> None:
       print(f"{msg.task} finished in {elapsed:.3f}s")

   @app.events.task_dead.connect
   def on_dead(msg) -> None:
       sentry_sdk.capture_message(f"task {msg.task} exhausted retries")

Handlers can be sync or async; exceptions are logged and swallowed so a bad
handler never brings down the worker.

Available signals and their handler signatures:

.. list-table::
   :header-rows: 1

   * - Signal
     - Handler arguments
   * - ``task_enqueued``
     - ``(msg: Message)``
   * - ``task_received``
     - ``(msg: Message)``
   * - ``task_started``
     - ``(msg: Message)``
   * - ``task_succeeded``
     - ``(msg: Message, elapsed: float)``
   * - ``task_failed``
     - ``(msg: Message, exc: BaseException)``
   * - ``task_retried``
     - ``(msg: Message, delay: float)``
   * - ``task_dead``
     - ``(msg: Message)``
   * - ``worker_heartbeat``
     - ``(msg: Message)``

Disconnect a handler with ``app.events.<signal>.disconnect(handler)``.

Watch / Hot Reload
------------------

.. code-block:: toml

   [watch]
   enable = true
   root = "."
   respect_gitignore = true
   exclude = [".git/**"]
   reload_delay = 0.25
   reload_debounce = 0.5

When enabled, the orchestrator watches ``root`` and reloads the worker pool
on settled change batches. ``.gitignore`` patterns and ``exclude`` globs
filter out noise (caches, build artifacts, IDE temp files).

The scheduler shares the orchestrator lifetime, so a reload restarts
in-memory ``every(...)`` jobs at ``now + interval``. Cron jobs re-anchor to
the next match and are unaffected.

CLI
---

Two commands:

.. code-block:: shell

   uv run kuu start
   uv run kuu start -c ./path/to/kuunfig.toml
   uv run kuu start -o concurrency=128 -o dashboard.enable=true
   uv run kuu info

``-o dotted.path=value`` is repeatable. Values parse as JSON when valid
(``true``, ``42``, ``["a","b"]``); otherwise they stay as strings.

See :doc:`config` for the full TOML schema.

Testing
-------

``MemoryBroker`` plus a programmatic ``Worker`` is the usual setup:

.. code-block:: python

   import anyio
   from kuu import Kuu
   from kuu.brokers.memory import MemoryBroker
   from kuu.config import Settings
   from kuu.worker import Worker


   async def test_charge_runs():
       app = Kuu(broker=MemoryBroker())

       @app.task
       async def charge(x: int) -> int:
           return x * 2

       handle = await charge.q(5)

       config = Settings.model_construct(queues=["default"], concurrency=4)
       worker = Worker(config, app=app)

       async with anyio.create_task_group() as tg:
           tg.start_soon(worker.run)
           # ...assert behavior, then cancel the worker
           tg.cancel_scope.cancel()

``Worker(config, app=app)`` skips the dotted-spec import so you can pass an
inline-built ``Kuu`` instance.
