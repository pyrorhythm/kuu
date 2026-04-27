from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from kuu.app import Kuu
    from kuu.message import Message
    from kuu.task import Task


type Phase = Literal["enqueue", "process"]


@dataclass
class Context:
    """
    Execution context for a task.

    Holds the app instance, message, phase, and mutable state.

    Attributes:
        app: the kuu application
        message: the current message
        phase: current lifecycle phase, either "enqueue" or "process"
        task: the task instance, if known
        state: mutable dict for middleware and handlers to share data
    """

    app: Kuu
    message: Message
    phase: Phase
    task: Task | None = None
    state: dict[str, Any] = field(default_factory=dict)
