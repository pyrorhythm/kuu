Config
======

Put the block below into ``kuunfig.toml``, or under ``[tool.kuu]`` in your
``pyproject.toml``.

The ``[default]`` section holds values shared across presets.
Each ``[presets.<name>]`` overrides only the fields you set; unset fields
fall back to ``[default]``.

Omitting ``[default]`` and writing a flat config is allowed for backward
compat (keys land in ``[default]`` automatically).

.. code-block:: toml

   [default]
       app = "src.core.broker:broker"
       task_modules = [
           #...
       ]
       queues = []
       processes = 2
       concurrency = 32
       prefetch = 16
       shutdown_timeout = 30.0
       scheduler.enable = false
       metrics.enable = true

   [default.dashboard]
       enable = true
       path = "/"

   [default.watch]
       enable = true
       root = "."
       respect_gitignore = true
       exclude = [".git/**"]
       reload_delay = 0.25
       reload_debounce = 0.5

   [presets.scheduler]
       task_modules = [
           #...
       ]
       processes = 2
       concurrency = 16
       scheduler.enable = true
       dashboard.enable = false

   [presets.message]
       app = "src.core.message_broker:message_broker"
       task_modules = [
           #...
       ]
       processes = 1
       concurrency = 16
       scheduler.enable = false
       dashboard.enable = false

CLI Overrides
-------------

Any setting can be overridden with ``-o dotted.path=value``, repeatable.
Pick a preset with ``-p`` / ``--preset``:

.. code-block:: shell

   uv run kuu start -p prod \
     -o concurrency=128 \
     -o dashboard.enable=true \
     -o queues='["high","low"]'

.. note::

   Overrides apply to the selected preset, not to ``[default]``.

Values parse as JSON when possible (``true``, ``42``, ``[1,2]``);
otherwise raw strings.
