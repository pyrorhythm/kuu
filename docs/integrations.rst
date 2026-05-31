Integrations
============

Integrations under ``kuu.contrib`` are optional. Each one has its own install
extra:

.. code-block:: shell

   uv add "kuu[otel]"        # OpenTelemetry traces, metrics, logs
   uv add "kuu[prometheus]"  # Prometheus metrics
   uv add "kuu[di]"          # Dishka dependency injection
   uv add "kuu[structlog]"   # structlog task logging

OpenTelemetry
-------------

``kuu.contrib.otel`` adds traces, metrics, and log correlation.
``KuuOTELInstrumentor`` sets up all three:

.. code-block:: python

   from kuu.contrib.otel import KuuOTELInstrumentor

   # Auto-configure the SDK from OTEL_EXPORTER_OTLP_ENDPOINT:
   KuuOTELInstrumentor(app=app).instrument()

   # Or reuse SDK providers you configured yourself:
   KuuOTELInstrumentor(app=app).instrument(setup_sdk=False)

``instrument()`` inserts ``OtelTracingMiddleware`` at the front of the chain (a
producer span on enqueue, a consumer span on process, with W3C trace-context
propagated through message headers), registers ``OtelMetrics`` on
``app.events``, and routes stdlib ``logging`` to the OTel log provider.

Pass your own exporters (for example gRPC):

.. code-block:: python

   from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

   KuuOTELInstrumentor(app=app, span_exporter=OTLPSpanExporter(...)).instrument()

Emitted metrics: ``kuu.task.enqueued``, ``kuu.task.processed`` (by ``status``),
``kuu.task.duration``, ``kuu.task.in_flight``, ``kuu.task.retried``. Call
``shutdown_telemetry()`` on shutdown to flush exporters. ``OtelTracingMiddleware``,
``OtelMetrics``, and ``OtelLoggingBridge`` can also be used on their own.

Prometheus
----------

There are two parts, both using ``app.events``: a worker-side emitter and
client-side middleware.

Worker metrics run in the control plane. With ``[metrics] enable = true`` the
orchestrator runs an aggregator HTTP server and each worker writes to a shared
multiprocess directory:

.. code-block:: toml

   [metrics]
   enable = true
   host = "0.0.0.0"
   port = 9191

Scrape ``http://host:9191/metrics``.

Client-side middleware records enqueue metrics from every ``task.q(...)`` call:

.. code-block:: python

   from kuu.contrib.prometheus import ClientMetrics

   app = Kuu(broker=..., middleware=[ClientMetrics()])

This emits ``client_enqueued_total``, ``client_enqueue_errors_total`` and
``client_enqueue_duration_seconds`` on the same registry the worker side uses.
Expose them from your own ASGI app:

.. code-block:: python

   from fastapi import FastAPI
   from kuu.contrib.prometheus import asgi_app

   api = FastAPI()
   api.mount("/metrics", asgi_app())

Dishka (Dependency Injection)
-----------------------------

``kuu.contrib.dishka`` resolves task dependencies from a `Dishka
<https://dishka.readthedocs.io>`_ container. Declare them on the task signature
and each run gets them from a fresh ``REQUEST`` scope.

Mark injected parameters with ``FromDishka()`` as the default value. The
dependency type comes from the annotation, so the parameter reads like a normal
typed argument. Apply ``@inject`` below ``@app.task`` and register the container
with ``setup_dishka``:

.. code-block:: python

   from dishka import Provider, Scope, make_async_container, provide
   from kuu import Kuu
   from kuu.contrib.dishka import FromDishka, inject, setup_dishka


   class QueryService: ...
   class WriteService: ...


   class AppProvider(Provider):
       scope = Scope.REQUEST
       read = provide(QueryService)
       write = provide(WriteService)


   app = Kuu(broker=...)
   setup_dishka(app, make_async_container(AppProvider()))


   @app.task
   @inject
   async def get_lot(
       lot_id: str,
       read: QueryService = FromDishka(),
       write: WriteService = FromDishka(),
       fetch: bool = True,
   ) -> str:
       ...

   # kuu sees only the business signature `(lot_id, fetch)`:
   await get_lot.q("lot-1", fetch=False)

``@inject`` removes the ``FromDishka()`` parameters from the public signature.
They are not serialised into the message or expected from the caller; the
container provides them when the worker runs the task. Without ``@inject`` the
worker rebinds the payload against the full signature and fails with
``Missing argument 'read'``. ``setup_dishka`` inserts ``KuuDishkaMiddleware`` at
the front of the chain, which opens a ``REQUEST`` scope around every task and
closes it afterwards.

Sync (blocking) tasks
~~~~~~~~~~~~~~~~~~~~~~

``blocking=True`` tasks run in a worker thread and need a **sync** container.
The choice is strict: async tasks resolve from ``container`` and blocking tasks
from ``sync_container``. Provide both if you mix them:

.. code-block:: python

   from dishka import make_container, make_async_container

   setup_dishka(
       app,
       make_async_container(AppProvider()),
       sync_container=make_container(AppProvider()),
   )

   @app.task(blocking=True)
   @inject
   def crunch(lot_id: str, read: QueryService = FromDishka()) -> int:
       ...

Using your own ContextVar
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you already keep the container in a ``ContextVar`` (a request middleware, a
shared web app), point ``inject`` at it with ``from_contextvar`` and skip the
kuu middleware. You manage the scope yourself:

.. code-block:: python

   from kuu.contrib.dishka import from_contextvar, inject

   from myapp.ioc import container_var  # ContextVar[AsyncContainer]

   @app.task
   @inject(container_getter=from_contextvar(container_var))
   async def handler(read: QueryService = FromDishka()) -> None:
       ...

To let kuu manage the scope but write the request container into your own var,
pass ``context_var=`` to the middleware and read the same var in ``inject``:

.. code-block:: python

   from kuu.contrib.dishka import KuuDishkaMiddleware, from_contextvar

   app.middleware.insert(0, KuuDishkaMiddleware(container, context_var=container_var))

The subscript form ``read: FromDishka[QueryService]`` also works, so existing
Dishka signatures keep running.

Structlog
---------

``kuu.contrib.structlog.StructlogMiddleware`` logs task start, success, and
failure through a ``structlog`` logger, during the ``process`` phase only.
Configure structlog yourself and pass a bound logger:

.. code-block:: python

   import structlog
   from kuu import Kuu
   from kuu.contrib.structlog import StructlogMiddleware

   structlog.configure(...)
   app = Kuu(broker=..., middleware=[StructlogMiddleware(structlog.get_logger())])

It emits ``task.start``, ``task.ok``, and ``task.fail`` events with
``task_name``, ``sched_id``, ``queue``, ``attempt``, ``duration`` and
``exc_type`` keys. It implements :class:`kuu.middleware.TaskLogSink`; implement
that protocol and drive it with :func:`kuu.middleware.run_process_task_logging`
to log through any other frontend.
