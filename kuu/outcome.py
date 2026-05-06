from dataclasses import dataclass


class Outcome: ...


@dataclass(frozen=True, slots=True)
class Ok(Outcome):
	time_elapsed: float


@dataclass(frozen=True, slots=True)
class Retry(Outcome):
	delay_seconds: float


@dataclass(frozen=True, slots=True)
class Reject(Outcome):
	requeue: bool


@dataclass(frozen=True, slots=True)
class Fail(Outcome):
	exc: BaseException
