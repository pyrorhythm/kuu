from __future__ import annotations

import os
import socket

from kuu.brokers.redis import RedisBroker


def test_default_consumer_name_is_unique_per_instance():
	a = RedisBroker()
	b = RedisBroker()

	prefix = f"{socket.gethostname()}.{os.getpid()}."
	assert a.consumer.startswith(prefix)
	assert b.consumer.startswith(prefix)
	assert a.consumer != b.consumer


def test_explicit_consumer_name_is_kept():
	assert RedisBroker(consumer="c1").consumer == "c1"
