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

> ### a *native* distributed task queue for python

```shell
uv add kuu
# extras: msgspec, nats, prometheus, redis, dashboard
```

nothing serious in this project, just got tired of taskiq's undefined behaviour and `logging.getLogger("root")`

## quick start

```python
# myapp/app.py
from kuu import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.results.redis import RedisResults

app = Kuu(broker=RedisBroker(url=...), results=RedisResults(url=...))


# myapp/tasks.py
from .app import app


class ChargeResult(TypedDict):
    ok: bool
    charged: int


@app.task
async def charge(user_id: int, amount_cents: int) -> ChargeResult:
    return {"ok": True, "charged": amount_cents}


@app.schedule.cron(expr="* * */4 * * *")
async def refresh_balance() -> None: ...


# myapp/main.py
from .tasks import charge


async def run() -> None:
    # type checker will automatically infer here
    # that handle is type of TaskHandle[ChargeResult]
    # args/kwargs of the task also remain typed
    handle = await charge.q(user_id=1, amount_cents=500)

    # type checker will infer ChargeResult
    result = await handle.result(timeout=30)
```

```sh
# reads ./kuunfig.toml or [tool.kuu] in ./pyproject.toml
uv run kuu start 

# reads ./path/to/kuunfig.toml and overriding specified settings
uv run kuu start -c ./path/to/kuunfig.toml -o concurrency=128 -o dashboard.enable=true
```

## config

put the block below into `kuunfig.toml`, or under `[tool.kuu]` in your `pyproject.toml`
every field except `app` and `task_modules` has a default and can be omitted

```toml
app = "myapp.module:instance"            # dotted path to the Kuu instance
task_modules = ["myapp.tasks"]           # modules that register tasks

queues = []                              # consume from; empty = auto-discover from registry
processes = 1                            # worker subprocesses to spawn
concurrency = 64                         # max concurrent tasks per worker
prefetch = 16                            # batch size; defaults to max(1, concurrency // 4)
shutdown_timeout = 30.0                  # seconds to wait for in-flight tasks on stop

[metrics]
enable = false
host = "0.0.0.0"
port = 9191

[dashboard]
enable = false
host = "0.0.0.0"
port = 8181
path = "/dashboard"

[scheduler]
enable = false                           # run scheduler loop in-process; jobs declared via app.schedule

[watch]
enable = false                           # reload workers on filesystem changes
root = "."                               # path to watch
respect_gitignore = true                 # skip files matched by .gitignore
exclude = [".git/**"]                    # extra globs to exclude
reload_delay = 0.25
reload_debounce = 0.5
```

any setting can be overridden from the CLI with `-o dotted.path=value`
values are parsed as JSON when possible (`true`, `42`, `["a","b"]`), otherwise kept as strings

## contribution

well if you insist... (issues / PRs welcome)

#clankersgoaway
