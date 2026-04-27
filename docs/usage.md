# Usage

A tour of Kuu's surface area, ordered roughly the way you'd reach for things.


## Defining the App

```python
from kuu import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.results.redis import RedisResults

app = Kuu(
    broker=RedisBroker(url="redis://localhost:6379/0"),
    default_queue="default",
    results=RedisResults(url="redis://localhost:6379/0"),
)
```

`Kuu` only owns transport (`broker`), result storage (`results`), middleware,
the task registry and the event bus.

## Tasks

### Declaring

```python
@app.task
async def charge(user_id: int, amount_cents: int) -> dict:
    return {"ok": True, "charged": amount_cents}
```

Add knobs through the parametrized form:

```python
@app.task(queue="payments", max_attempts=3, timeout=10.0)
async def charge(user_id: int, amount_cents: int) -> dict: ...
```

### Sync Functions

Sync functions work too. Pass `blocking=True` so the worker offloads them
to a thread instead of running on the event loop:

```python
@app.task(blocking=True)
def render_pdf(invoice_id: int) -> bytes: ...
```

Combining `blocking=True` with an `async def` raises `TypeError`. Async
functions never need it.

### Calling

Three ways:

```python
# 1) in-process direct call (no broker hop)
result = await charge(user_id=1, amount_cents=500)

# 2) enqueue and forget
handle = await charge.q(user_id=1, amount_cents=500)

# 3) enqueue and wait for the result
result = await handle.result(timeout=30)
```

`charge.q(...)` returns a `TaskHandle[ChargeResult]` typed off the function's
return annotation, so `result` keeps the right type without casts.

### Idempotency

Pass `idempotency_key` in headers to deduplicate against the result backend:

```python
await app.enqueue_by_name(
    "myapp.tasks:charge",
    Payload(args=(1, 500)),
    headers={"idempotency_key": "charge:order-42"},
)
```

If a result already exists at that key, the worker short-circuits and ack's
without running the task.

## Brokers

Three are bundled. They all implement the same `Broker` Protocol; pick one
based on the deployment.

### Redis Streams

```python
from kuu.brokers.redis import RedisBroker

broker = RedisBroker(
    url="redis://localhost:6379/0",
    group="payments",
    consumer="payments-1",
)
```

Live messages flow through Streams; scheduled (`not_before`) messages live
in a sorted set and are pumped into the live stream when due. Stale pending
entries from dead consumers are reclaimed automatically.

### NATS JetStream

```python
from kuu.brokers.nats import NatsBroker

broker = NatsBroker(
    servers="nats://localhost:4222",
    stream="kuu",
)
```

### In-Memory

For tests and single-process setups:

```python
from kuu.brokers.memory import MemoryBroker

broker = MemoryBroker(buffer=1024)
```

## Result Backend

Results are optional. If you set one, every task return value is persisted
under the message id (or `idempotency_key` when present), and `TaskHandle.result()`
can fetch it.

```python
from kuu.results.redis import RedisResults

results = RedisResults(
    url="redis://localhost:6379/0",
    prefix="kuu:r:",
    ttl=86400,  # entries expire after a day; None means no expiry
    replay=True,  # short-circuit on cache hit
    store_errors=True,  # persist terminal failures so callers can observe them
)
```

`replay=True` is the trick that makes idempotency_key-based deduplication
work end-to-end: the worker checks the backend before running, and if a
result is already present, it ack's the message and skips execution.

## Middleware

Three are bundled, same Protocol as user middleware:

```python
from kuu.middleware import LoggingMiddleware, RetryMiddleware, TimeoutMiddleware

app = Kuu(
    broker=...,
    middleware=[
        LoggingMiddleware(),
        RetryMiddleware(base=0.5, cap=60.0, jitter=0.2),
        TimeoutMiddleware(seconds=30.0),
    ],
)
```

- `LoggingMiddleware`: one line on start, one on success/failure with duration.
- `RetryMiddleware`: catches `RetryErr` (and uncaught exceptions, up to
  `max_attempts`), schedules the next attempt with exponential backoff plus
  jitter.
- `TimeoutMiddleware`: per-task `Task.timeout` wins; this `seconds` is the
  default.

### Custom Middleware

```python
class HeaderInjector:
    async def __call__(self, ctx, call_next):
        if ctx.phase == "enqueue":
            ctx.message.headers["x-tenant"] = current_tenant()
        return await call_next()
```

`ctx.phase` is `"enqueue"` for the publish path and `"process"` for the
worker path. Most middleware filters on phase.

## Scheduler

Jobs are declared in code via `app.schedule`. The scheduler loop runs in
the orchestrator process when `[scheduler] enable = true`.

```python
from datetime import timedelta


@app.task
async def refresh_balance() -> None: ...


# fixed-rate every 5 minutes
app.schedule.every(timedelta(minutes=5), task=refresh_balance)

# cron-style: every 4 hours, on the hour
app.schedule.cron(task=refresh_balance, expr="0 0 */4 * * *")

# structured cron without writing the expression
app.schedule.cron(task=refresh_balance, hour=[3, 15], minute=0)
```

The structured form rejects passing both `expr` and a field; it also fills
in zeros for unspecified smaller fields so `cron(hour=10)` means
`"0 0 10 * * *"`, not "every minute of every 10am".

## Prometheus Metrics

Two integration points: a worker-side metrics emitter and a client-side
middleware. They both wire onto `app.events`.

### Worker Metrics

When `[metrics] enable = true`, the orchestrator runs an aggregator HTTP
server on `metrics.port` and tells each worker subprocess to write to a
shared multiprocess directory.

```toml
[metrics]
enable = true
host = "0.0.0.0"
port = 9191
```

Hit `http://host:9191/metrics`.

### Client-Side

```python
from kuu.prometheus import ClientMetrics

app = Kuu(broker=..., middleware=[ClientMetrics()])
```

This records `client_enqueued_total`, `client_enqueue_errors_total` and
`client_enqueue_duration_seconds` from every `task.q(...)` call, on the
same registry the worker side uses.

To expose them from your own ASGI app:

```python
from fastapi import FastAPI
from kuu.prometheus import asgi_app

api = FastAPI()
api.mount("/metrics", asgi_app())
```

## Dashboard

Optional, requires the `dashboard` extra. Renders a small Starlette app
showing registered tasks, scheduled jobs, broker depth and live worker count.

```toml
[dashboard]
enable = true
host = "0.0.0.0"
port = 8181
path = "/dashboard"
```

The orchestrator imports the Kuu app via `config.app`, mounts the dashboard
under `path` and serves it via uvicorn alongside the worker pool.

## Watch / Hot Reload

```toml
[watch]
enable = true
root = "."
respect_gitignore = true
exclude = [".git/**"]
reload_delay = 0.25
reload_debounce = 0.5
```

When enabled, the orchestrator runs `watchfiles` against `root` and reloads
the worker pool on every settled change batch. Patterns from `.gitignore`
plus `exclude` globs are folded into the watch filter so noise (caches,
build artifacts, IDE temp files) does not trigger reloads.

The scheduler shares the orchestrator lifetime, so a watcher reload also
restarts in-memory `every(...)` jobs at `now + interval`. Cron jobs
re-anchor to the next match and are unaffected.

## CLI

Two commands matter day-to-day:

```sh
uv run kuu start                                            # default config
uv run kuu start -c ./path/to/kuunfig.toml                  # explicit
uv run kuu start -o concurrency=128 -o dashboard.enable=true  # ad-hoc overrides
uv run kuu info                                             # show resolved config
```

`-o dotted.path=value` is repeatable. Values parse as JSON when valid
(`true`, `42`, `["a","b"]`); otherwise they stay as strings.

See {doc}`config` for the full TOML schema.

## Testing

`MemoryBroker` plus a programmatic Worker is the usual setup:

```python
import anyio
from kuu import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.config import Kuunfig
from kuu.worker import Worker


async def test_charge_runs():
    app = Kuu(broker=MemoryBroker())

    @app.task
    async def charge(x: int) -> int:
        return x * 2

    handle = await charge.q(5)

    config = Kuunfig.model_construct(queues=["default"], concurrency=4)
    worker = Worker(config, app=app)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker.run)
        # ...assert behavior, then cancel the worker
        tg.cancel_scope.cancel()
```

`Worker(config, app=app)` skips the dotted-spec import so you can pass an
inline-built `Kuu` instance.
