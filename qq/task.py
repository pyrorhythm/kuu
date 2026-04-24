from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass
class Task:
    name: str
    queue: str
    fn: Callable[..., Awaitable[Any]]
    args_model: type[BaseModel] | None
    max_attempts: int = 5
    timeout: float | None = None


def infer_args_model(fn: Callable[..., Any]) -> type[BaseModel] | None:
    sig = inspect.signature(fn)
    for p in sig.parameters.values():
        if p.annotation is inspect.Parameter.empty:
            continue
        ann = p.annotation
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            return ann
    return None


class Registry:
    def __init__(self) -> None:
        self._by_name: dict[str, Task] = {}

    def add(self, task: Task) -> None:
        if task.name in self._by_name:
            raise ValueError(f"duplicate task name: {task.name}")
        self._by_name[task.name] = task

    def get(self, name: str) -> Task | None:
        return self._by_name.get(name)

    def names(self) -> list[str]:
        return list(self._by_name)

    def queues(self) -> set[str]:
        return {t.queue for t in self._by_name.values()}
