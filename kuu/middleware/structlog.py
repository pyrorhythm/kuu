from __future__ import annotations

import warnings

from kuu.contrib.structlog import StructlogMiddleware

__all__ = ("StructlogMiddleware",)

warnings.warn(
	"`kuu.middleware.structlog` has moved to `kuu.contrib.structlog`; import from "
	"there instead. This module is scheduled to be completely removed in 0.3.0.",
	DeprecationWarning,
	stacklevel=2,
)
