from dataclasses import dataclass


class Outcome: ...


@dataclass
class Ok(Outcome):
    time_elapsed: float


@dataclass
class Retry(Outcome):
    delay_seconds: float


@dataclass
class Reject(Outcome):
    requeue: bool


@dataclass
class Fail(Outcome):
    exc: BaseException
