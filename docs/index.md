
```{figure} _static/kuu-logo-slogan.svg
:align: center
:figclass: only-light header-slogan
```

```{figure} _static/kuu-logo-slogan-white.svg
:align: center
:figclass: only-dark header-slogan
```
# kuu 
> ## **/kuː/** finnish, noun - moon

*A native distributed task queue*, simple to drop into production.

```{toctree}
:maxdepth: 2
:hidden:

quickstart
usage
config
apidocs/index
```

## Install

```sh
uv add kuu
# extras: msgspec, nats, prometheus, redis, dashboard
```

## At a Glance

```python
from kuu import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.results.redis import RedisResults

app = Kuu(broker=RedisBroker(url=...), results=RedisResults(url=...))


@app.task
async def charge(user_id: int, amount_cents: int) -> dict:
    return {"ok": True, "charged": amount_cents}
```

```sh
uv run kuu start
```

Go to {doc}`quickstart` for the full pass, {doc}`usage` for a tour of the package, {doc}`config` for tunables, or {doc}`apidocs/index` for the API reference.

## Indices

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
