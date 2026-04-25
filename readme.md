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