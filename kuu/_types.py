import functools
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Coroutine, Protocol

if TYPE_CHECKING:
	from kuu.task import Task


class Connectable(Protocol):
	async def connect(self) -> None: ...


type _FnAsync[**P, R] = Callable[P, Coroutine[Any, Any, R]]
type _Fn[**P, R] = Callable[P, R] | Callable[P, Coroutine[Any, Any, R]]
type _FnSingle[P, R] = Callable[[P], R]
type _Wrap[**P, R] = _FnSingle[_Fn[P, R], Task[P, R]]


def _ensure_connected[T: Connectable, **P, R](
	fn: Callable[Concatenate[T, P], Coroutine[Any, Any, R]],
) -> Callable[Concatenate[T, P], Coroutine[Any, Any, R]]:
	@functools.wraps(fn)
	async def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
		await self.connect()
		return await fn(self, *args, **kwargs)

	return wrapper
