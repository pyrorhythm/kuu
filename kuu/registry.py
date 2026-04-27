from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kuu.task import Task


class Registry:
    def __init__(self) -> None:
        self._by_name: dict[str, Task] = {}

    def add(self, task: Task) -> None:
        if task.task_name in self._by_name:
            raise ValueError(f"duplicate task name: {task.task_name}")
        self._by_name[task.task_name] = task

    def get(self, name: str) -> Task | None:
        return self._by_name.get(name)

    def names(self) -> list[str]:
        return list(self._by_name)

    def queues(self) -> set[str]:
        return {t.task_queue for t in self._by_name.values()}
