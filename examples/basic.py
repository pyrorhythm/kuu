from __future__ import annotations

import anyio
from pydantic import BaseModel

from qq import Q
from qq.brokers.redis import RedisBroker
from qq.middleware import RetryMiddleware, TimeoutMiddleware
from qq.worker import Worker


app = Q(
    broker=RedisBroker(url="redis://localhost:6379/0"),
    middleware=[RetryMiddleware(base=0.5, cap=30), TimeoutMiddleware(seconds=30)],
)


class ChargeArgs(BaseModel):
    user_id: int
    amount_cents: int


@app.task(name="billing.charge", queue="billing", max_attempts=5)
async def charge(args: ChargeArgs) -> dict:
    return {"ok": True, "user": args.user_id, "amount": args.amount_cents}


@app.events.task_succeeded.connect
async def _on_ok(msg, duration):
    print(f"ok {msg.task} in {duration*1000:.1f}ms")


async def produce() -> None:
    await app.broker.connect()
    try:
        await app.enqueue("billing.charge", ChargeArgs(user_id=1, amount_cents=4200))
    finally:
        await app.broker.close()


async def consume() -> None:
    await Worker(app, concurrency=32).run()


if __name__ == "__main__":
    import sys
    anyio.run(consume if sys.argv[-1] == "worker" else produce)
