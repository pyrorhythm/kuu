"""Run the agreed local observability baseline: 10k Runs and 10k records/s."""

from __future__ import annotations

import tempfile
import time
from datetime import timedelta
from pathlib import Path

import anyio

from kuu._util import utcnow
from kuu.config import PersistenceConfig
from kuu.observability import PROTOCOL_VERSION, BrowserStream, Envelope, Event
from kuu.persistence import LogRow, LogicalRunRow
from kuu.persistence._sqlite import SqliteBackend

COUNT = 10_000


async def main() -> None:
	with tempfile.TemporaryDirectory() as directory:
		backend = SqliteBackend(
			PersistenceConfig(enable=True, dsn=f"sqlite:///{Path(directory) / 'bench.db'}")
		)
		await backend.connect()
		await backend.init_schema()
		now = utcnow()
		runs = [
			LogicalRunRow(
				message_id=f"run-{index}",
				task="benchmark.task",
				queue="default",
				instance_id="benchmark",
				status="running",
				created_at=now,
				updated_at=now,
			)
			for index in range(COUNT)
		]
		started = time.perf_counter()
		await backend.write_logical_runs(runs)
		run_rate = COUNT / (time.perf_counter() - started)

		logs = [
			LogRow(
				message_id=f"run-{index}",
				attempt=0,
				message="telemetry",
			)
			for index in range(COUNT)
		]
		started = time.perf_counter()
		await backend.write_logs(logs)
		log_rate = COUNT / (time.perf_counter() - started)

		stream = BrowserStream()
		started = time.perf_counter()
		for index in range(COUNT):
			stream.publish(
				Envelope(
					v=PROTOCOL_VERSION,
					instance="benchmark",
					ts=now + timedelta(microseconds=index),
					body=Event(
						kind="started",
						task="benchmark.task",
						queue="default",
						worker_pid=1,
						message_id=f"run-{index}",
						attempt=0,
					),
				)
			)
		stream_rate = COUNT / (time.perf_counter() - started)
		page = await backend.query_logical_runs(limit=100)
		await backend.close()

	print(f"logical Run upserts: {run_rate:,.0f}/s")
	print(f"observation writes:  {log_rate:,.0f}/s")
	print(f"stream routing:      {stream_rate:,.0f}/s")
	print(f"active Runs retained: {COUNT:,}; first page: {len(page)}")
	if min(run_rate, log_rate, stream_rate) < 10_000 or len(page) != 100:
		raise SystemExit("baseline failed")


if __name__ == "__main__":
	anyio.run(main)
