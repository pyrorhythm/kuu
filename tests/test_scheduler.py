from __future__ import annotations

from datetime import datetime, timedelta, timezone

from kuu.app import Kuu
from kuu.brokers.memory import MemoryBroker
from kuu.scheduler import IntervalJob, Scheduler
from kuu.scheduler.scheduler import _build_expr


def test_structured_kwargs_apply_smaller_field_zero_default():
    expr = _build_expr(
        second=None,
        minute=None,
        hour=10,
        day=None,
        month=None,
        day_of_week=None,
        second_interval=None,
        minute_interval=None,
        hour_interval=None,
    )
    assert expr == "0 0 10 * * *"


def test_hour_interval_shorthand_expands_to_step_form():
    expr = _build_expr(
        second=None,
        minute=None,
        hour=None,
        day=None,
        month=None,
        day_of_week=None,
        second_interval=None,
        minute_interval=None,
        hour_interval=4,
    )
    assert expr == "0 0 */4 * * *"


def test_specifying_both_field_and_interval_is_rejected():
    import pytest

    with pytest.raises(ValueError, match="hour"):
        _build_expr(
            second=None,
            minute=None,
            hour=10,
            day=None,
            month=None,
            day_of_week=None,
            second_interval=None,
            minute_interval=None,
            hour_interval=4,
        )


def test_list_field_expands_to_comma_separated():
    expr = _build_expr(
        second=None,
        minute=[0, 15, 30, 45],
        hour=None,
        day=None,
        month=None,
        day_of_week=None,
        second_interval=None,
        minute_interval=None,
        hour_interval=None,
    )
    assert expr == "0 0,15,30,45 * * * *"


def test_cron_first_run_is_in_the_future():
    app = Kuu(broker=MemoryBroker())
    sched = Scheduler(app)
    before = datetime.now(timezone.utc)
    job = sched.cron(task="t", hour=before.hour + 1 if before.hour < 23 else 0)
    # whatever the next match is, it must be strictly after `before`
    assert job.next_run > before


def test_every_first_run_is_now_plus_interval():
    app = Kuu(broker=MemoryBroker())
    sched = Scheduler(app)
    before = datetime.now(timezone.utc)
    job = sched.every(timedelta(seconds=10), task="t")
    # first run anchored to now+interval (no immediate_start anymore)
    assert (job.next_run - before).total_seconds() >= 9.5


def test_kuu_exposes_schedule_attribute():
    app = Kuu(broker=MemoryBroker())
    assert isinstance(app.schedule, Scheduler)
    assert app.schedule.app is app


def test_scheduler_runner_is_noop_when_disabled():
    """regression: orchestrator must skip scheduler entirely when disabled."""
    import anyio
    from kuu.config import Kuunfig
    from kuu.orchestrator._scheduler import SchedulerRunner

    cfg = Kuunfig.model_construct(app="x:y", task_modules=[])
    # scheduler.enable defaults to False
    runner = SchedulerRunner(cfg)
    # returns immediately without trying to import x:y
    anyio.run(runner.run, anyio.Event())


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
