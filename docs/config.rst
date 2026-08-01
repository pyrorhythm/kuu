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

   [default.persistence]
       enable = true
       dsn = "sqlite:///./kuu.db"
       schema = ""
       runs_table = "kuu_runs"
       logical_runs_table = "kuu_logical_runs"
       logs_table = "kuu_run_logs"
       keep_days = 7
       max_runs = 100_000
       log_level = "INFO"
       capture_args = false
       capture_headers = false
       capture_result = false
       preview_bytes = 16_384
       attempt_observation_bytes = 10_485_760
       trace_url_template = ""

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
