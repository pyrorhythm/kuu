from __future__ import annotations

import warnings

from kuu.contrib.otel import (
	KuuOTELInstrumentor,
	OtelLoggingBridge,
	OtelMetrics,
	OtelTracingMiddleware,
	shutdown_telemetry,
)

__all__ = (
	"KuuOTELInstrumentor",
	"OtelTracingMiddleware",
	"OtelMetrics",
	"OtelLoggingBridge",
	"shutdown_telemetry",
)

warnings.warn(
	"`kuu.otel` has moved to `kuu.contrib.otel`; import from there instead. "
	"This module is scheduled to be completely removed in 0.3.0.",
	DeprecationWarning,
	stacklevel=2,
)
