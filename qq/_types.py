from typing import TYPE_CHECKING, Callable, Coroutine

if TYPE_CHECKING:
	from .task import Task

type _FnAsync[**P, R] = Callable[P, Coroutine[None, None, R]]
type _Fn[**P, R] = Callable[P, R] | _FnAsync[P, R]
type _FnS[P, R] = Callable[[P], R]
type _Wrap[**P, R] = _FnS[_Fn[P, R], Task[P, R]]
