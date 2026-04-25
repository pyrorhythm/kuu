from __future__ import annotations

import pytest

from qq.app import Q
from qq.brokers.memory import MemoryBroker


@pytest.fixture
def app() -> Q:
	return Q(broker=MemoryBroker())
