class TaskError(Exception):
	pass


class RetryErr(TaskError):
	def __init__(self, delay: float | None = None, reason: str | None = None):
		super().__init__(reason or "retry")
		self.delay = delay


class RejectErr(TaskError):
	def __init__(self, reason: str | None = None, requeue: bool = False):
		super().__init__(reason or "reject")
		self.requeue = requeue


class UnknownTask(TaskError): ...


class NotConnected(TaskError): ...


class InvalidReceiptType(TaskError): ...
