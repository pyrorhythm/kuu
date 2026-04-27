# Config

Put the block below into `kuunfig.toml`, or under `[tool.kuu]` in your `pyproject.toml`. Every field except `app` and `task_modules` has a default and may be omitted.

```toml
app = "myapp.module:instance"            # dotted path to the Kuu instance
task_modules = ["myapp.tasks"]           # modules that register tasks

queues = []                              # consume from; empty = auto-discover
processes = 1                            # worker subprocesses to spawn
concurrency = 64                         # max concurrent tasks per worker
prefetch = 16                            # batch size; defaults to max(1, concurrency // 4)
shutdown_timeout = 30.0                  # seconds to wait for in-flight tasks on stop

[scheduler]
enable = false                           # run scheduler loop in-process
                                         # jobs declared in code via app.schedule

[metrics]
enable = false
host = "0.0.0.0"
port = 9191

[dashboard]
enable = false
host = "0.0.0.0"
port = 8181
path = "/dashboard"

[watch]
enable = false                           # reload workers on filesystem changes
root = "."                               # path to watch
respect_gitignore = true                 # skip files matched by .gitignore
exclude = [".git/**"]                    # extra globs to exclude
reload_delay = 0.25
reload_debounce = 0.5
```

## CLI Overrides

Any setting can be overridden with `-o dotted.path=value`, repeatable:

```sh
uv run kuu start \
  -o concurrency=128 \
  -o dashboard.enable=true \
  -o queues='["high","low"]'
```

Values parse as JSON when possible (`true`, `42`, `[1,2]`); otherwise raw strings.
