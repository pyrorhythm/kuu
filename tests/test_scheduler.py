from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.scheduler import IntervalJob, ScheduleJob, Scheduler
from kuu.scheduler.schedule import Fri, Mon, Wed, at, between, every, in_month, on, on_day


def test_interval_job_skips_missed_runs_after_long_pause():
	t0 = datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc)
	job = IntervalJob(
		id="j",
		task_name="t",
		every=timedelta(seconds=10),
		next_run=t0,
	)
	now = t0 + timedelta(seconds=95)
	job.schedule_next(now)
	assert job.next_run > now
	assert (job.next_run - t0).total_seconds() == 100


def test_interval_job_first_run_anchored_to_add_time():
	before = datetime.now(timezone.utc)
	app = Kuu(broker=MemoryBroker())
	sched = Scheduler(app)
	job = sched.add_every(timedelta(seconds=10), "t")
	assert (job.next_run - before).total_seconds() >= 9.5


def test_schedule_job_next_run_is_in_future():
	app = Kuu(broker=MemoryBroker())
	sched = Scheduler(app)
	before = datetime.now(timezone.utc)
	# Pick an hour in the future
	target_hour = before.hour + 1 if before.hour < 23 else 0
	job = sched.add_schedule(at(time(target_hour, 0)), "t")
	assert job.next_run > before


def test_at_next_after():
	s = at(time(9, 30))
	now = datetime(2026, 4, 29, 8, 0, tzinfo=timezone.utc)
	nxt = s.next_after(now)
	assert nxt.hour == 9
	assert nxt.minute == 30
	assert nxt.day == 29


def test_at_next_after_same_day_past():
	s = at(time(9, 30))
	now = datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc)
	nxt = s.next_after(now)
	assert nxt.day == 30  # next day


def test_on_matches_correct_weekdays():
	s = on(Mon, Wed, Fri)
	assert s.matches(datetime(2026, 4, 27, tzinfo=timezone.utc))  # Monday
	assert not s.matches(datetime(2026, 4, 28, tzinfo=timezone.utc))  # Tuesday
	assert s.matches(datetime(2026, 4, 29, tzinfo=timezone.utc))  # Wednesday


def test_on_next_after():
	s = on(Mon)
	# Tuesday April 28, 2026
	now = datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
	nxt = s.next_after(now)
	assert nxt.weekday() == 0  # Monday
	assert nxt > now


def test_every_hours_fires_at_interval():
	s = every(hours=4, starting=time(0, 0))
	now = datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc)
	nxt = s.next_after(now)
	assert nxt.hour == 8  # 00, 04, 08, 12, 16, 20


def test_every_matches():
	s = every(hours=4, starting=time(0, 0))
	assert s.matches(datetime(2026, 4, 29, 8, 0, tzinfo=timezone.utc))
	assert not s.matches(datetime(2026, 4, 29, 9, 0, tzinfo=timezone.utc))


def test_on_day_matches():
	s = on_day(1, 15)
	assert s.matches(datetime(2026, 4, 1, tzinfo=timezone.utc))
	assert s.matches(datetime(2026, 4, 15, tzinfo=timezone.utc))
	assert not s.matches(datetime(2026, 4, 10, tzinfo=timezone.utc))


def test_in_month_matches():
	from kuu.scheduler.schedule.enums import Apr, Jul

	s = in_month(Apr, Jul)
	assert s.matches(datetime(2026, 4, 1, tzinfo=timezone.utc))
	assert s.matches(datetime(2026, 7, 15, tzinfo=timezone.utc))
	assert not s.matches(datetime(2026, 5, 10, tzinfo=timezone.utc))


def test_between_constrains_to_window():
	s = between(time(9), time(17))
	inside = datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc)
	outside_before = datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc)
	outside_after = datetime(2026, 4, 29, 18, 0, tzinfo=timezone.utc)
	assert s.matches(inside)
	assert not s.matches(outside_before)
	assert not s.matches(outside_after)


def test_and_composition():
	s = on(Mon, Wed, Fri) & at(time(9, 0))
	assert s.matches(datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc))  # Monday
	assert not s.matches(datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc))  # Tuesday
	assert not s.matches(datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc))


def test_or_composition():
	s = at(time(9, 0)) | at(time(17, 0))
	assert s.matches(datetime(2026, 4, 29, 9, 0, tzinfo=timezone.utc))
	assert s.matches(datetime(2026, 4, 29, 17, 0, tzinfo=timezone.utc))
	assert not s.matches(datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc))


def test_scheduler_add_every_returns_interval_job():
	app = Kuu(broker=MemoryBroker())
	sched = Scheduler(app)
	job = sched.add_every(timedelta(minutes=5), "myapp.tasks:refresh")
	assert isinstance(job, IntervalJob)
	assert job.task_name == "myapp.tasks:refresh"
	assert job.every == timedelta(minutes=5)


def test_scheduler_add_schedule_returns_schedule_job():
	app = Kuu(broker=MemoryBroker())
	sched = Scheduler(app)
	job = sched.add_schedule(at(time(3, 0)), "myapp.tasks:cleanup")
	assert isinstance(job, ScheduleJob)
	assert job.task_name == "myapp.tasks:cleanup"


def test_scheduler_run_is_noop_when_no_jobs():
	import anyio

	app = Kuu(broker=MemoryBroker())
	sched = Scheduler(app)

	async def _sched_run() -> None:
		await sched.run(install_signals=False)

	async def _run_and_cancel():
		async with anyio.create_task_group() as tg:
			tg.start_soon(_sched_run)
			await anyio.sleep(0.05)
			tg.cancel_scope.cancel()

	anyio.run(_run_and_cancel)


def test_kuu_exposes_schedule_attribute():
	app = Kuu(broker=MemoryBroker())
	assert isinstance(app.schedule, Scheduler)
	assert app.schedule.app is app


def test_kuu_every_decorator_registers_interval_job():
	app = Kuu(broker=MemoryBroker())

	@app.every(timedelta(minutes=5))
	async def refresh():
		pass

	assert len(app.schedule.jobs) == 1
	job = app.schedule.jobs[0]
	assert isinstance(job, IntervalJob)
	assert job.every == timedelta(minutes=5)
	assert "refresh" in job.task_name


def test_kuu_sched_decorator_registers_schedule_job():
	app = Kuu(broker=MemoryBroker())

	@app.sched(at(time(9, 0)) & on(Mon, Wed, Fri))
	async def morning_report():
		pass

	assert len(app.schedule.jobs) == 1
	job = app.schedule.jobs[0]
	assert isinstance(job, ScheduleJob)
	assert "morning_report" in job.task_name


def test_kuu_every_decorator_accepts_optional_kwargs():
	app = Kuu(broker=MemoryBroker())

	@app.every(
		timedelta(seconds=30),
		id="custom-id",
		queue="high",
		headers={"x-priority": "1"},
		max_attempts=2,
	)
	async def urgent():
		pass

	job = app.schedule.jobs[0]
	assert job.id == "custom-id"
	assert job.queue == "high"
	assert job.headers == {"x-priority": "1"}
	assert job.max_attempts == 2


def test_scheduler_runner_is_noop_when_disabled():
	import anyio

	from kuu.config import Settings
	from kuu.orchestrator._scheduler import SchedulerRunner

	cfg = Settings.model_construct(app="x:y", task_modules=[])
	# scheduler.enable defaults to False
	runner = SchedulerRunner(cfg)
	# returns immediately without trying to import x:y
	anyio.run(runner.run, anyio.Event())
