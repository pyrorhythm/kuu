from __future__ import annotations

import pytest

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker


@pytest.fixture
def app() -> Kuu:
	return Kuu(broker=MemoryBroker())
