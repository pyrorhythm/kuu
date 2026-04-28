from ..result import Result
from .base import ResultBackend

__all__ = ["Result", "ResultBackend"]

try:
	from .redis import RedisResults

	__all__ += ["RedisResults"]
except ImportError:
	pass
