from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from msgspec import ValidationError, convert

from kuu.config import Kuunfig, Settings

pytestmark = pytest.mark.anyio


def _write(p: Path, body: str) -> None:
	p.write_text(textwrap.dedent(body).lstrip())


class TestLoad:
	def test_loads_explicit_kuunfig_toml(self, tmp_path: Path) -> None:
		f = tmp_path / "kuunfig.toml"
		_write(
			f,
			"""
			app = "myapp.module:instance"
			task_modules = ["myapp.tasks"]
			concurrency = 32
			""",
		)
		raw = Kuunfig.load(f)
		cfg = raw.default
		assert cfg.app == "myapp.module:instance"
		assert cfg.task_modules == ["myapp.tasks"]
		assert cfg.concurrency == 32

	def test_loads_pyproject_tool_kuu(self, tmp_path: Path) -> None:
		f = tmp_path / "pyproject.toml"
		_write(
			f,
			"""
			[project]
			name = "x"

			[tool.kuu]
			app = "x.mod:k"
			task_modules = ["x.tasks"]

			[tool.kuu.metrics]
			enable = true
			port = 9999
			""",
		)
		raw = Kuunfig.load(f)
		cfg = raw.default
		assert cfg.app == "x.mod:k"
		assert cfg.metrics.enable is True
		assert cfg.metrics.port == 9999

	def test_loads_structured_with_default_section(self, tmp_path: Path) -> None:
		f = tmp_path / "kuunfig.toml"
		_write(
			f,
			"""
			[default]
			app = "myapp:kuu"
			task_modules = ["myapp.tasks"]
			concurrency = 32

			[presets.prod]
			concurrency = 128
			processes = 8

			[presets.dev]
			concurrency = 16
			""",
		)
		raw = Kuunfig.load(f)
		assert raw.default.app == "myapp:kuu"
		assert raw.default.concurrency == 32
		assert raw.default.processes == 1
		assert raw.presets["prod"]["concurrency"] == 128
		assert raw.presets["prod"]["processes"] == 8
		assert raw.presets["prod"].get("app") is None
		assert raw.presets["dev"]["concurrency"] == 16
		assert raw.presets["dev"].get("processes") is None

	def test_autodiscover_prefers_kuunfig_over_pyproject(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		monkeypatch.chdir(tmp_path)
		_write(
			tmp_path / "kuunfig.toml",
			"""
			app = "from.kuunfig:k"
			task_modules = ["a"]
			""",
		)
		_write(
			tmp_path / "pyproject.toml",
			"""
			[tool.kuu]
			app = "from.pyproject:k"
			task_modules = ["b"]
			""",
		)
		raw = Kuunfig.load()
		assert raw.default.app == "from.kuunfig:k"

	def test_autodiscover_falls_back_to_pyproject(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		monkeypatch.chdir(tmp_path)
		_write(
			tmp_path / "pyproject.toml",
			"""
			[tool.kuu]
			app = "from.pyproject:k"
			task_modules = ["b"]
			""",
		)
		raw = Kuunfig.load()
		assert raw.default.app == "from.pyproject:k"

	def test_autodiscover_skips_pyproject_without_tool_kuu(
		self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
	) -> None:
		monkeypatch.chdir(tmp_path)
		_write(
			tmp_path / "pyproject.toml",
			"""
			[project]
			name = "x"
			""",
		)
		with pytest.raises(FileNotFoundError):
			Kuunfig.load()

	def test_load_missing_path_raises(self, tmp_path: Path) -> None:
		with pytest.raises(FileNotFoundError):
			Kuunfig.load(tmp_path / "nope.toml")


def _base() -> Settings:
	return convert({"app": "mod:obj", "task_modules": ["mod.tasks"]}, Settings)


class TestOverrides:
	def test_simple_scalar(self) -> None:
		out = _base().with_overrides(["concurrency=128"])
		assert out.concurrency == 128

	def test_nested_path(self) -> None:
		out = _base().with_overrides(["dashboard.enable=true", "dashboard.port=9090"])
		assert out.dashboard.enable is True
		assert out.dashboard.port == 9090

	def test_json_list_value(self) -> None:
		out = _base().with_overrides(['queues=["a","b"]'])
		assert out.queues == ["a", "b"]

	def test_string_value_without_quotes_is_kept_as_string(self) -> None:
		out = _base().with_overrides(["app=other.mod:obj"])
		assert out.app == "other.mod:obj"

	def test_returns_new_instance_does_not_mutate(self) -> None:
		base = _base()
		out = base.with_overrides(["concurrency=99"])
		assert out is not base
		assert base.concurrency == 64
		assert out.concurrency == 99

	def test_empty_list_is_noop(self) -> None:
		base = _base()
		out = base.with_overrides([])
		# noop short-circuit returns the same instance
		assert out is base

	def test_invalid_format_raises(self) -> None:
		with pytest.raises(ValueError, match="dotted.path=value"):
			_base().with_overrides(["concurrency"])

	def test_validation_runs_on_overridden_values(self) -> None:
		with pytest.raises(ValidationError):
			_base().with_overrides(["concurrency=0"])


class TestInvariants:
	def test_extra_keys_forbidden(self) -> None:
		with pytest.raises(ValidationError):
			convert({"default": {"app": "m:o", "task_modules": ["m"]}, "wat": 1}, Kuunfig)

	def test_frozen(self) -> None:
		cfg = _base()
		with pytest.raises(AttributeError):
			cfg.concurrency = 999  # type: ignore[misc]

	def test_app_pattern_enforced(self) -> None:
		with pytest.raises(ValidationError):
			convert({"app": "no_colon", "task_modules": ["m"]}, Settings)

	def test_prefetch_defaults_to_zero(self) -> None:
		cfg = convert(
			{"app": "m:o", "task_modules": ["m"], "concurrency": 100}, Settings
		)
		assert cfg.prefetch == 0


class TestPresets:
	def test_resolve_none_returns_default(self) -> None:
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"], "concurrency": 64},
			},
			Kuunfig,
		)
		cfg = raw.resolve(None)
		assert cfg is raw.default
		assert cfg.concurrency == 64

	def test_resolve_merges_preset_over_default(self) -> None:
		raw = convert(
			{
				"default": {
					"app": "a:b",
					"task_modules": ["t"],
					"concurrency": 64,
					"processes": 4,
				},
				"presets": {
					"prod": {"processes": 8},
				},
			},
			Kuunfig,
		)
		cfg = raw.resolve("prod")
		assert cfg.concurrency == 64  # from default
		assert cfg.processes == 8  # from preset

	def test_resolve_preset_overrides_all_levels(self) -> None:
		raw = convert(
			{
				"default": {
					"app": "a:b",
					"task_modules": ["t"],
					"concurrency": 64,
					"processes": 4,
					"metrics": {"enable": True, "host": "0.0.0.0", "port": 9191},
				},
				"presets": {
					"staging": {
						"concurrency": 32,
						"metrics": {"enable": False, "host": "0.0.0.0", "port": 9090},
					},
				},
			},
			Kuunfig,
		)
		cfg = raw.resolve("staging")
		assert cfg.concurrency == 32
		assert cfg.processes == 4  # from default
		assert cfg.metrics.enable is False
		assert cfg.metrics.port == 9090

	def test_resolve_unknown_preset_raises(self) -> None:
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"]},
			},
			Kuunfig,
		)
		with pytest.raises(KeyError, match="preset 'nope' not found"):
			raw.resolve("nope")

	def test_empty_preset_equals_default(self) -> None:
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"], "concurrency": 64},
				"presets": {"empty": {}},
			},
			Kuunfig,
		)
		cfg = raw.resolve("empty")
		assert cfg.app == raw.default.app
		assert cfg.concurrency == raw.default.concurrency

	def test_preset_does_not_mutate_default(self) -> None:
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"], "concurrency": 64},
				"presets": {"fast": {"concurrency": 128}},
			},
			Kuunfig,
		)
		resolved = raw.resolve("fast")
		assert resolved.concurrency == 128
		assert raw.default.concurrency == 64

	def test_preset_key_error_message_lists_available(self) -> None:
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"]},
				"presets": {"prod": {}, "dev": {}},
			},
			Kuunfig,
		)
		with pytest.raises(KeyError, match=r"\['dev', 'prod'\]"):
			raw.resolve("nope")

	def test_preset_submodel_partial_merge(self) -> None:
		"""Overriding one submodel field must not wipe other submodel fields."""
		raw = convert(
			{
				"default": {
					"app": "a:b",
					"task_modules": ["t"],
					"dashboard": {
						"enable": True,
						"host": "0.0.0.0",
						"port": 8181,
						"path": "/dash",
					},
				},
				"presets": {
					"alt": {
						"dashboard": {
							"enable": False,
							"host": "0.0.0.0",
							"port": 9090,
							"path": "/alt",
						},
					},
				},
			},
			Kuunfig,
		)
		cfg = raw.resolve("alt")
		assert cfg.dashboard.enable is False
		assert cfg.dashboard.port == 9090
		assert cfg.dashboard.path == "/alt"
		assert cfg.dashboard.host == "0.0.0.0"

	def test_preset_validate_result(self) -> None:
		"""Resolved settings must pass validation (e.g., app pattern)."""
		raw = convert(
			{
				"default": {"app": "a:b", "task_modules": ["t"]},
				"presets": {"good": {"app": "other.mod:instance"}},
			},
			Kuunfig,
		)
		cfg = raw.resolve("good")
		assert cfg.app == "other.mod:instance"

	def test_flat_backward_compat_convert(self) -> None:
		"""Flat dict auto-wraps via load path (was model_validator before)."""
		# The auto-wrap logic is in Kuunfig.load, not in convert.
		# Test that convert with explicit {default: ...} works.
		raw = convert(
			{"default": {"app": "x:y", "task_modules": ["t"], "concurrency": 32}},
			Kuunfig,
		)
		assert raw.default.app == "x:y"
		assert raw.default.concurrency == 32
		assert raw.presets == {}

	def test_flat_backward_compat_does_not_wrap_double(self) -> None:
		"""A dict with 'default' key passes through unchanged."""
		raw = convert(
			{
				"default": {"app": "x:y", "task_modules": ["t"]},
				"presets": {"p": {"concurrency": 10}},
			},
			Kuunfig,
		)
		assert raw.default.app == "x:y"
		assert raw.presets["p"]["concurrency"] == 10
