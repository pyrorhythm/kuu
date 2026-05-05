import inspect
import typing
import uuid

import orjson
from starlette.requests import Request
from starlette.responses import JSONResponse

from kuu.app import Kuu
from kuu.message import Payload
from kuu.observability import EnqueueCmd, RemoveJobCmd, TriggerJobCmd
from kuu.orchestrator import PresetSupervisor
from kuu.scheduler import Scheduler
from kuu.web.stats import StatsCollector

if typing.TYPE_CHECKING:
	from kuu.orchestrator._control import ControlPlane

_SIMPLE_TYPES = (bool, int, float, str)


def _sig_params(fn: typing.Any) -> tuple[list[dict], bool]:
	"""returns (params_list, has_varargs) for a callable"""
	sig = inspect.signature(fn)
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	params: list[dict] = []
	has_varargs = False
	for name, param in sig.parameters.items():
		if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
			has_varargs = True
			continue
		ann = hints.get(name, param.annotation)
		if ann is inspect.Parameter.empty:
			ann_str = None
		elif isinstance(ann, type):
			ann_str = ann.__name__
		else:
			ann_str = str(ann).replace("typing.", "")
		required = param.default is inspect.Parameter.empty
		default: typing.Any = None
		if not required:
			try:
				orjson.dumps(param.default)
				default = param.default
			except (TypeError, ValueError):
				default = repr(param.default)
		params.append({
			"name": name,
			"annotation": ann_str,
			"default": default,
			"required": required,
		})
	return params, has_varargs


def _validate_payload(fn: typing.Any, args: list, kwargs: dict) -> str | None:
	sig = inspect.signature(fn)
	try:
		hints = typing.get_type_hints(fn)
	except Exception:
		hints = {}
	try:
		bound = sig.bind(*args, **kwargs)
	except TypeError as exc:
		return str(exc)
	bound.apply_defaults()
	for name, value in bound.arguments.items():
		ann = hints.get(name)
		if ann is None:
			continue
		if getattr(ann, "__origin__", None) is not None:
			continue
		if not isinstance(ann, type) or ann not in _SIMPLE_TYPES:
			continue
		# bool is a subclass of int; check it before int
		if ann is int and isinstance(value, bool):
			return f"parameter {name!r}: expected int, got bool"
		if not isinstance(value, ann):
			return f"parameter {name!r}: expected {ann.__name__}, got {type(value).__name__}"
	return None


class DashbordAPIMixin:
	app: Kuu
	scheduler: Scheduler | None = None
	orchestrator: PresetSupervisor | None = None
	control: "ControlPlane | None" = None
	stats: StatsCollector

	async def _api_activity(self, request: Request) -> JSONResponse:
		return JSONResponse(self.stats.activity_series())

	async def _api_task_params(self, request: Request) -> JSONResponse:
		task_name = request.query_params.get("task")
		if not task_name:
			return Err("task required")
		task = self.app.registry.get(task_name)
		if not task:
			return Err("task not found", 404)
		params, has_varargs = _sig_params(task.original_func)
		return Ok({"params": params, "raw": has_varargs})

	async def _api_run_task(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		task_name = body.get("task")
		if not task_name:
			return Err("task required")
		raw_args = body.get("args")
		raw_kwargs = body.get("kwargs")
		if not isinstance(raw_args, list) or not isinstance(raw_kwargs, dict):
			return Err("args must be array, kwargs must be object")
		task = self.app.registry.get(task_name)
		if not task:
			return Err("task not found", 404)
		err = _validate_payload(task.original_func, raw_args, raw_kwargs)
		if err:
			return Err(err, 422)

		instance = body.get("instance")
		if self.control is not None and instance:
			cmd = EnqueueCmd(
					request_id=uuid.uuid4().hex,
					task=task_name,
					args=raw_args,
					kwargs=raw_kwargs,
			)
			return await _route(self.control, instance, cmd)

		try:
			await self.app.enqueue_by_name(
					task_name, Payload(args=tuple(raw_args), kwargs=raw_kwargs)
			)
			return Ok()
		except Exception as exc:
			return Err(str(exc))

	async def _api_trigger_job(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		job_id = body.get("job_id")
		if not job_id:
			return Err("job_id required")

		instance = body.get("instance")
		if self.control is not None and instance:
			cmd = TriggerJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, instance, cmd)

		if not self.scheduler:
			return Err("no scheduler")
		job = next((j for j in self.scheduler.jobs if j.id == job_id), None)
		if not job:
			return Err("job not found", 404)
		try:
			await self.app.enqueue_by_name(
					job.task_name,
					job.args,
					queue=job.queue,
					headers=job.headers,
					max_attempts=job.max_attempts,
			)
			return Ok()
		except Exception as exc:
			return Err(str(exc))

	async def _api_remove_job(self, request: Request) -> JSONResponse:
		try:
			body = await request.json()
		except Exception:
			return Err("invalid json")
		job_id = body.get("job_id")
		if not job_id:
			return Err("job_id required")

		instance = body.get("instance")
		if self.control is not None and instance:
			cmd = RemoveJobCmd(request_id=uuid.uuid4().hex, job_id=job_id)
			return await _route(self.control, instance, cmd)

		if not self.scheduler:
			return Err("no scheduler")
		before = len(self.scheduler.jobs)
		self.scheduler.jobs = [j for j in self.scheduler.jobs if j.id != job_id]
		if len(self.scheduler.jobs) == before:
			return Err("job not found", 404)
		return Ok()


async def _route(control: "ControlPlane", instance: str, cmd: typing.Any) -> JSONResponse:
	try:
		resp = await control.send_command(instance, cmd)
	except KeyError:
		return Err("unknown instance", 404)
	if resp.ok:
		return Ok()
	return Err(resp.error or "command failed")


class Ok(JSONResponse):
	def __init__(self, content: typing.Any = None, **kwargs):
		super().__init__(content if content else {"ok": True}, **kwargs)


class Err(JSONResponse):
	def __init__(self, error: str, status_code: int = 400, **kwargs):
		content = {"error": error}
		super().__init__(content, status_code, **kwargs)
