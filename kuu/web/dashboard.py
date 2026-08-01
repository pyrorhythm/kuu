from __future__ import annotations

import typing
from pathlib import Path

import anyio

from jinja2 import Environment, FileSystemLoader, select_autoescape

from kuu.marshal import marshal as _marshal
from kuu.persistence import PersistenceBackend, PersistenceWorker
from starlette.applications import Starlette
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket

from kuu._util import utcnow
from kuu.app import Kuu
from kuu.observability import (
	Bye,
	Cmd,
	CmdResponse,
	Envelope,
	Event,
	Hello,
	InstanceRegistry,
	LogBatch,
	State,
	BrowserStream,
	command_response_from_bytes,
	command_to_bytes,
	envelope_from_bytes,
)
from kuu.orchestrator.main import PresetSupervisor
from kuu.scheduler.scheduler import Scheduler
from kuu.web.api import DashbordAPIMixin
from kuu.web.fragments import DashboardFragmentsMixin
from kuu.web.stats import StatsCollector

UPLINK_LOST_AFTER = 5.0


if typing.TYPE_CHECKING:
	from kuu.orchestrator._control import ControlPlane


class Dashboard(DashboardFragmentsMixin, DashbordAPIMixin):
	def __init__(
		self,
		app: Kuu | None = None,
		scheduler: Scheduler | None = None,
		orchestrator: PresetSupervisor | None = None,
		registry: InstanceRegistry | None = None,
		control: "ControlPlane | None" = None,
		title: str = "kuu dashboard",
		ingest_token: str | None = None,
		persistence_backend: PersistenceBackend | None = None,
		persistence_worker: PersistenceWorker | None = None,
		trace_url_template: str | None = None,
	) -> None:
		self.app = app
		self.scheduler = scheduler
		self.orchestrator = orchestrator
		self.registry = registry
		self.control = control
		if self.control is None and registry is not None:
			self.control = typing.cast("ControlPlane", self)
		self.title = title
		self.persistence_backend = persistence_backend
		self.persistence_worker = persistence_worker
		self.trace_url_template = trace_url_template
		self._ingest_token = ingest_token
		self.browser_stream = BrowserStream()
		self._uplinks: dict[str, tuple[WebSocket, anyio.Lock]] = {}
		self._remote_pending: dict[str, tuple[anyio.Event, CmdResponse | None, str, WebSocket]] = {}
		self.stats = StatsCollector(app, connect_app_events=registry is None and app is not None)
		here = Path(__file__).parent
		self.jinja = Environment(
			loader=FileSystemLoader(str(here / "templates")),
			autoescape=select_autoescape(["html", "xml"]),
		)
		self.jinja.filters["tojson"] = lambda v: _marshal.json_encode(v).decode()

	def build_app(self) -> Starlette:
		static_dir = Path(__file__).parent / "static"
		return Starlette(
			debug=False,
			routes=[
				Route("/", self._index),
				Route("/fragments/stats", self._frag_stats),
				Route("/fragments/tasks", self._frag_tasks),
				Route("/fragments/task-runs", self._frag_task_runs),
				Route("/fragments/task-run-detail", self._frag_task_run_detail),
				Route("/fragments/scheduler", self._frag_scheduler),
				Route("/fragments/presets", self._frag_presets),
				Route("/fragments/queues", self._frag_queues),
				Route("/api/activity", self._api_activity),
				Route("/api/task-params", self._api_task_params),
				Route("/api/run-task", self._api_run_task, methods=["POST"]),
				Route("/api/cancel-run", self._api_cancel_run, methods=["POST"]),
				Route("/api/retry-run", self._api_retry_run, methods=["POST"]),
				Route("/api/replay-run", self._api_replay_run, methods=["POST"]),
				Route("/api/trigger-job", self._api_trigger_job, methods=["POST"]),
				Route("/api/remove-job", self._api_remove_job, methods=["POST"]),
				Route("/api/task-runs", self._api_task_runs),
				Route("/api/task-run-attempts", self._api_task_run_attempts),
				Route("/api/task-run-logs", self._api_task_run_logs),
				WebSocketRoute("/_ingest", self._ws_ingest),
				WebSocketRoute("/ws", self._ws_browser),
				Mount("/static", StaticFiles(directory=str(static_dir)), name="static"),
			],
		)

	def ingest_envelope(self, env: Envelope) -> None:
		if self.registry is not None:
			self.registry.ingest(env)
		self.browser_stream.publish(env)
		match env.body:
			case Event() as e:
				self.stats.ingest(e.kind, e.task, env.ts)
				if self.persistence_worker is not None:
					self.persistence_worker.enqueue_event(env.instance, e)
			case LogBatch() as batch:
				if self.persistence_worker is not None:
					self.persistence_worker.enqueue_log_batch(env.instance, batch)
			case Hello() | State() | Bye():
				pass
			case _:
				pass

	async def _ws_browser(self, websocket: WebSocket) -> None:
		await self.browser_stream.connect(websocket)

	async def _ws_ingest(self, websocket: WebSocket) -> None:
		if not self._authorized(websocket):
			await websocket.close(code=4401)
			return
		await websocket.accept()
		bound_instance: str | None = None
		timed_out = False
		try:
			while True:
				with anyio.move_on_after(UPLINK_LOST_AFTER) as timeout:
					data = await websocket.receive_bytes()
				if timeout.cancel_called:
					timed_out = True
					await websocket.close(code=4408, reason="uplink heartbeat timeout")
					return
				try:
					env = envelope_from_bytes(data)
				except Exception as exc:
					if "unsupported observability protocol" in str(exc):
						await websocket.close(code=4406, reason=str(exc))
						return
					if bound_instance is None:
						await websocket.close(code=4400, reason="protocol v2 hello required")
						return
					try:
						self._resolve_remote_response(
							command_response_from_bytes(data), bound_instance, websocket
						)
					except Exception:
						pass
					continue
				if bound_instance is None and not isinstance(env.body, Hello):
					await websocket.close(code=4400, reason="protocol v2 hello required")
					return
				if bound_instance is not None and env.instance != bound_instance:
					await websocket.close(code=4400, reason="uplink instance changed")
					return
				if isinstance(env.body, Hello):
					bound_instance = env.instance
					self._uplinks[env.instance] = (websocket, anyio.Lock())
				self.ingest_envelope(env)
		except Exception:
			pass
		finally:
			disconnected: list[str] = []
			for instance, (connected, _) in tuple(self._uplinks.items()):
				if connected is websocket:
					self._uplinks.pop(instance, None)
					disconnected.append(instance)
			try:
				await websocket.close()
			except Exception:
				pass
			if self.persistence_worker is not None and disconnected:
				if not timed_out:
					await anyio.sleep(UPLINK_LOST_AFTER)
				for instance in disconnected:
					if instance not in self._uplinks:
						try:
							await self.persistence_worker.backend.mark_instance_unknown(
								instance, utcnow()
							)
						except Exception:
							pass

	async def send_command(self, target: str, cmd: Cmd, timeout: float = 10.0) -> CmdResponse:
		instance = self._resolve_target(target)
		connection = self._uplinks.get(instance)
		if connection is None:
			raise KeyError(target)
		websocket, lock = connection
		rid = getattr(cmd, "request_id", "")
		event = anyio.Event()
		self._remote_pending[rid] = (event, None, instance, websocket)
		try:
			async with lock:
				await websocket.send_bytes(command_to_bytes(cmd))
			with anyio.fail_after(timeout):
				await event.wait()
		except TimeoutError:
			return CmdResponse(request_id=rid, ok=False, error="timeout")
		except Exception as exc:
			return CmdResponse(request_id=rid, ok=False, error=f"send failed: {exc}")
		finally:
			pending = self._remote_pending.pop(rid, None)
		if pending is None or pending[1] is None:
			return CmdResponse(request_id=rid, ok=False, error="connection lost")
		return pending[1]

	def _resolve_target(self, target: str) -> str:
		if target in self._uplinks:
			return target
		if self.registry is not None:
			matches = [
				entry
				for entry in self.registry.all()
				if entry.hello.preset == target and entry.instance_id in self._uplinks
			]
			if matches:
				return max(matches, key=lambda entry: entry.last_seen).instance_id
		raise KeyError(target)

	def _resolve_remote_response(
		self, response: CmdResponse, instance: str, websocket: WebSocket
	) -> None:
		pending = self._remote_pending.get(response.request_id)
		if pending is None:
			return
		event, _, expected_instance, expected_websocket = pending
		if instance != expected_instance or websocket is not expected_websocket:
			return
		self._remote_pending[response.request_id] = (
			event,
			response,
			expected_instance,
			expected_websocket,
		)
		event.set()

	def _authorized(self, websocket: WebSocket) -> bool:
		if self._ingest_token is None:
			return True
		auth = websocket.headers.get("authorization", "")
		expected = f"Bearer {self._ingest_token}"
		return auth == expected

	def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
		import uvicorn

		uvicorn.run(self.build_app(), host=host, port=port, log_level="warning")

	async def start_server(self, host: str = "127.0.0.1", port: int = 8000) -> None:
		import uvicorn

		cfg = uvicorn.Config(self.build_app(), host=host, port=port, log_level="warning")
		await uvicorn.Server(cfg).serve()
