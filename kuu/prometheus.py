from __future__ import annotations

import warnings

from kuu.contrib.prometheus import (
	ClientMetrics,
	WorkerMetrics,
	asgi_app,
	mark_worker_dead,
	serve,
)

__all__ = (
	"WorkerMetrics",
	"ClientMetrics",
	"serve",
	"asgi_app",
	"mark_worker_dead",
)

warnings.warn(
	"`kuu.prometheus` has moved to `kuu.contrib.prometheus`; import from there "
	"instead. This module is scheduled to be completely removed in 0.3.0.",
	DeprecationWarning,
	stacklevel=2,
)
