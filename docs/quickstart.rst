Quickstart
==========

.. code-block:: python

   # myapp/app.py
   from kuu import Kuu
   from kuu.brokers.redis import RedisBroker
   from kuu.results.redis import RedisResults

   app = Kuu(broker=RedisBroker(url=...), results=RedisResults(url=...))


   # myapp/tasks.py
   from datetime import timedelta
   from typing import TypedDict

   from .app import app


   class ChargeResult(TypedDict):
       ok: bool
       charged: int


   @app.task
   async def charge(user_id: int, amount_cents: int) -> ChargeResult:
       return {"ok": True, "charged": amount_cents}


   @app.every(timedelta(hours=4))
   async def refresh_balance() -> None: ...


   # myapp/main.py
   from .tasks import charge


   async def run() -> None:
       handle = await charge.q(user_id=1, amount_cents=500)
       result = await handle.result(timeout=30)

.. code-block:: shell

   uv run kuu start

``kuu start`` reads ``./kuunfig.toml`` first, falling back to ``[tool.kuu]``
in ``./pyproject.toml``. Override any field from the CLI:

.. code-block:: shell

   uv run kuu start -c ./path/to/kuunfig.toml -o concurrency=128 -o dashboard.enable=true

Values are parsed as JSON when possible (``true``, ``42``, ``["a","b"]``);
otherwise treated as strings.
