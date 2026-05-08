.. figure:: _static/kuu-logo-slogan.svg
   :align: center
   :figclass: only-light header-slogan

.. figure:: _static/kuu-logo-slogan-white.svg
   :align: center
   :figclass: only-dark header-slogan

kuu
===

  /kuː/ - Finnish, noun. Moon.

A native distributed task queue for Python.

.. code-block:: shell

   uv add kuu
   # extras: msgspec, nats, prometheus, redis, dashboard

.. toctree::
   :maxdepth: 2
   :hidden:

   quickstart
   usage
   config
   apidocs/index

At a Glance
-----------

.. code-block:: python

   from kuu import Kuu
   from kuu.brokers.redis import RedisBroker
   from kuu.results.redis import RedisResults

   app = Kuu(broker=RedisBroker(url=...), results=RedisResults(url=...))


   @app.task
   async def charge(user_id: int, amount_cents: int) -> dict:
       return {"ok": True, "charged": amount_cents}

.. code-block:: shell

   uv run kuu start

See :doc:`quickstart` for setup, :doc:`usage` for the full API tour,
:doc:`config` for tunables, or :doc:`apidocs/index` for the reference.

Indices
-------

- :ref:`genindex`
- :ref:`modindex`
- :ref:`search`
