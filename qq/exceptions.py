class TaskError(Exception):
    pass


class Retry(TaskError):
    def __init__(self, delay: float | None = None, reason: str | None = None):
        super().__init__(reason or "retry")
        self.delay = delay


class Reject(TaskError):
    def __init__(self, reason: str | None = None, requeue: bool = False):
        super().__init__(reason or "reject")
        self.requeue = requeue


class UnknownTask(TaskError): ...


class NotConnected(TaskError): ...
