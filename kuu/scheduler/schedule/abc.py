from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta


class Schedule(ABC):
	@abstractmethod
	def next_after(self, now: datetime) -> datetime: ...

	@abstractmethod
	def matches(self, dt: datetime) -> bool: ...

	def __and__(self, other: Schedule) -> _and:
		left = list(self.parts) if isinstance(self, _and) else [self]
		right = list(other.parts) if isinstance(other, _and) else [other]
		return _and(*left, *right)

	def __or__(self, other: Schedule) -> _or:
		return _or(self, other)


class _and(Schedule):
	def __init__(self, *parts: Schedule) -> None:
		self.parts = parts

	def matches(self, dt: datetime) -> bool:
		return all(p.matches(dt) for p in self.parts)

	def next_after(self, now: datetime) -> datetime:
		deadline = now + timedelta(days=366)
		candidate = now

		while candidate < deadline:
			for i, driver in enumerate(self.parts):
				nxt = driver.next_after(candidate)
				others = [self.parts[j] for j in range(len(self.parts)) if j != i]
				if all(o.matches(nxt) for o in others):
					return nxt

			candidate = max(p.next_after(candidate) for p in self.parts)
			if self.matches(candidate):
				return candidate

		raise RuntimeError("no schedule match found within 366 days")


class _or(Schedule):
	def __init__(self, *parts: Schedule) -> None:
		self.parts = parts

	def matches(self, dt: datetime) -> bool:
		return any(p.matches(dt) for p in self.parts)

	def next_after(self, now: datetime) -> datetime:
		return min(p.next_after(now) for p in self.parts)
