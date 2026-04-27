from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from kuu.config import Kuunfig


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
        cfg = Kuunfig.load(f)
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
        cfg = Kuunfig.load(f)
        assert cfg.app == "x.mod:k"
        assert cfg.metrics.enable is True
        assert cfg.metrics.port == 9999

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
        cfg = Kuunfig.load()
        assert cfg.app == "from.kuunfig:k"

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
        cfg = Kuunfig.load()
        assert cfg.app == "from.pyproject:k"

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


def _base() -> Kuunfig:
    return Kuunfig.model_validate({"app": "mod:obj", "task_modules": ["mod.tasks"]})


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
        # 0 should fail validation
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            _base().with_overrides(["concurrency=0"])


class TestInvariants:
    def test_extra_keys_forbidden(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Kuunfig.model_validate({"app": "m:o", "task_modules": ["m"], "wat": 1})

    def test_frozen(self) -> None:
        from pydantic import ValidationError

        cfg = _base()
        with pytest.raises(ValidationError):
            cfg.concurrency = 999  # type: ignore[misc]

    def test_app_pattern_enforced(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Kuunfig.model_validate({"app": "no_colon", "task_modules": ["m"]})

    def test_prefetch_default_derived_from_concurrency(self) -> None:
        cfg = Kuunfig.model_validate({"app": "m:o", "task_modules": ["m"], "concurrency": 100})
        assert cfg.prefetch == 25
