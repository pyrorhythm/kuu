# kuu

```shell
uv install kuu --prerelease=allow

# extras availible: msgspec, nats, prometheus, redis
```

a native distributed queue that is rather simple and easy-to-integrate in production
has task queue, redis xstream, nats jetstream support, result backend, middlewares and event/signals
also has a built-in scheduler yk

nothing serious in this project, just got tired of taskiq's undefined behaviour and `logging.getLogger("root")`
maybe will build a webui for it idk

## quick start

> myapp/app.py
```python
from kuu import Kuu
from kuu.brokers.redis import RedisBroker
from kuu.middleware import RetryMiddleware, LoggingMiddleware, TimeoutMiddleware
from kuu.prometheus import ClientMetrics
from kuu.results.redis import RedisResults
from sotkalib.log import configure_logging

configure_logging()

app = Kuu(
	broker=RedisBroker(uri=...),
	default_queue="some-obscure-name-for-queue",
	results=RedisResults(uri=...),
	middleware=[
		LoggingMiddleware(...),
		RetryMiddleware(...),
		TimeoutMiddleware(...),
	],
)
```

> myapp/task.py
```python
from .app import app


class ChargeResult(BaseModel):
	ok: bool
	charged: int


@app.task
async def charge(user_id: int, amount_cents: int) -> ChargeResult:
	return ChargeResult(ok=True, charged=amount_cents)
```

> myapp/main.py
```py
from .task import charge

async def some_func(...) -> ...:
	handle: TaskHandle[ChargeResult] = await charge.q(user_id=..., amount_cents=...)
	result: ChargeResult = await handle.result(timeout=30) # raises if could not get result in that time
```

```sh
# spin up worker
uv run kuu worker myapp.app:app myapp.task --concurrency=... -w
```

and now, when you will execute `charge.q`, it would offload to broker -> redis -> worker -> result backend

## prometheus

**kuu** has native integration for Prometheus metrics for both client and worker. 

### client-side
```py
app = Kuu(
	broker=RedisBroker(uri=...),
	default_queue="some-obscure-name-for-queue",
	results=RedisResults(uri=...),
	middleware=[
		LoggingMiddleware(...),
		RetryMiddleware(...),
		TimeoutMiddleware(...),
		ClientMetrics(
			...
		),  # By specifying this middleware, metrics were bound to kuu`s event system.
	],
)

"""To obtain these metrics, you could:"""

# 1) If you don't have/want to integrate with existing HTTP server (e.g.) FastAPI

from kuu.prometheus import serve

wsgi, thread = serve(port=...)

# This is nonblocking because server is launched in thread

# 2) ASGI integration
from kuu.prometheus import asgi_app

api_app = FastAPI()

# ...

api_app.mount("/metrics", asgi_app())
```

### server-side

```sh
# when launching workers, specify --metrics-port to serve prometheus metrics on it

uv run kuu worker {{...}} --metrics-port=9090

# now they`re availible at ::1:9090/metrics !
```

see [this file](./kuu/prometheus.py) for implementation.

## contribution

well if you insist
just use issues/pull requests and we will go from here
#clankersgoaway