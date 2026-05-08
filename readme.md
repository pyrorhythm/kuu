<p align="center">
  <img src=".github/logo/svg/kuu-logo-slogan-white.svg" alt="kuu" width="50%"/>
  <br/><br/>
  <a href="https://pypi.org/project/kuu/"><img src="https://img.shields.io/pypi/v/kuu?color=blue" alt="PyPI"/></a>
  <a href="https://pypi.org/project/kuu/"><img src="https://img.shields.io/pypi/pyversions/kuu" alt="Python"/></a>
  <a href="https://github.com/pyrorhythm/kuu/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/kuu" alt="License"/></a>
  <a href="https://pypi.org/project/kuu/"><img src="https://img.shields.io/pypi/dm/kuu" alt="Downloads"/></a>
</p>

<h1 align="center">kuu</h1>

---

> ### a _native_ distributed task queue for python

```shell
uv add kuu
# extras: dashboard, nats, postgres, prometheus, redis
```

## quick start

```python
# myapp/app.py
from kuu import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.results.redis import RedisResults

app = Kuu(broker=RedisBroker(url=...), results=RedisResults(url=...))


# myapp/tasks.py
from typing import TypedDict
from datetime import timedelta
from .app import app


class ChargeResult(TypedDict):
    ok: bool
    charged: int


@app.task
async def charge(user_id: int, amount_cents: int) -> ChargeResult:
    return {"ok": True, "charged": amount_cents}


@app.sched(every(hours=4, starting=time(hours=1, minutes=30))) # 1:30, 5:30, 9:30...
async def refresh_balance() -> None: ...


# myapp/main.py
from .tasks import charge


async def run() -> None:
    # type checker infers TaskHandle[ChargeResult]
    # args/kwargs of the task remain typed
    handle = await charge.q(user_id=1, amount_cents=500)

    # type checker infers ChargeResult
    result = await handle.result(timeout=30)
```

```sh
# reads ./kuunfig.toml or [tool.kuu] in ./pyproject.toml
# starts control plane with all presets spawned
uv run kuu start

# singular preset with dashboard / remote uplink
uv run kuu start --preset ...
```

## what's inside

- **brokers**: Redis Streams, NATS JetStream, in-memory (for tests)
- **scheduler**: interval jobs (`@app.every`) and composable cron-like schedules (`@app.sched`)
- **middleware**: logging, retry with exponential backoff + jitter, timeout, plus custom hooks
- **events**: pub/sub signals for task lifecycle (`task_enqueued` .. `task_dead`)
- **serialization**: JSON (msgspec), Msgpack, Pickle, with extensible type coercion via `marshal`
- **persistence**: SQLite (zero-config) and PostgreSQL backends for run/log history
- **dashboard**: Starlette+HTMX web UI with live worker/queue stats and task management
- **prometheus**: multiprocess metrics with worker-side emitter and client-side middleware
- **hot reload**: watch filesystem changes, restart worker pool on settled batches

## config

put the block below into `kuunfig.toml`, or under `[tool.kuu]` in your `pyproject.toml`

`[default]` holds base values; each `[presets.<name>]` overrides only the fields you set.
unset fields fall back to `[default]`. a flat config (no `[default]` wrapper) still works

```toml
[default]
queues = []              # consume from; empty = auto-discover from registry
processes = 1            # worker subprocesses to spawn
concurrency = 64         # max concurrent tasks per worker
prefetch = 16            # batch size; defaults to max(1, concurrency // 4)
shutdown_timeout = 30.0  # seconds to wait for in-flight tasks on stop

[default.metrics]
enable = false
host = "0.0.0.0"
port = 9191

[default.dashboard]
enable = false
host = "0.0.0.0"
port = 8181
path = "/dashboard"

scheduler.enable = false    # run scheduler loop in-process; jobs declared via app.every / app.sched

[default.watch]
enable = false              # reload workers on filesystem changes
root = "."                  # path to watch
respect_gitignore = true    # skip files matched by .gitignore
exclude = [".git/**"]       # extra globs to exclude
reload_delay = 0.25
reload_debounce = 0.5

[default.persistence]
enable = true               # store run/log history
dsn = "sqlite:///./kuu.db"  # sqlite (default) or postgres://...;
# also can be provided via KUU_PERSISTENCE_DSN env var
schema = ""                 # postgres schema; empty = default
runs_table = "kuu_runs"
logs_table = "kuu_run_logs"
keep_days = 7               # auto-purge runs older than this
max_runs = 100_000          # hard cap on stored runs
log_level = "INFO"
capture_args = true         # capture task args/kwargs in run detail

[presets.prod]
processes = 8
concurrency = 256

[presets.dev]
processes = 1
concurrency = 16
```

any setting can be overridden from the CLI with `-o dotted.path=value`
values are parsed as JSON when possible (`true`, `42`, `["a","b"]`), otherwise kept as strings

## contribution

well if you insist... (issues / PRs welcome)

#clankersgoaway
