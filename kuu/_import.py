from __future__ import annotations

import logging
import os
import sys
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def add_cwd_in_path() -> Generator[None]:
    """
    Temporarily prepend the current working directory to `sys.path`.

    Lets `import_module` find user code without requiring the project to
    be installed as a package.
    """
    cwd = Path.cwd()
    if str(cwd) in sys.path:
        yield
    else:
        sys.path.insert(0, str(cwd))
        try:
            yield
        finally:
            try:
                sys.path.remove(str(cwd))
            except ValueError:
                logger.warning(f"cannot remove '{cwd}' from sys.path")


def import_object(object_spec: str, app_dir: str | None = None) -> Any:
    """
    Parse a `module:attribute` spec and return the imported attribute.

    - `object_spec`: dotted path in the form `package.module:variable`.
    - `app_dir`: optional extra directory to prepend to `sys.path`.

    Raises `ValueError` if the spec is missing the `:` separator.
    """
    import_spec = object_spec.split(":")
    if len(import_spec) != 2:
        raise ValueError("you should provide object path in `module:variable` format.")
    with add_cwd_in_path():
        if app_dir:
            sys.path.insert(0, app_dir)
        module = import_module(import_spec[0])
    return getattr(module, import_spec[1])


def import_from_modules(modules: list[str]) -> None:
    """Import each module by name. Failures are logged, not raised."""
    for module in modules:
        try:
            with add_cwd_in_path():
                import_module(module)
        except ImportError as err:
            logger.warning(f"cannot import {module}. Cause:")
            logger.exception(err)


def import_tasks(modules: list[str], pattern: str | Sequence[str], fs_discover: bool) -> None:
    """
    Import the modules that register tasks via `@app.task`.

    - `modules`: explicit module list to import.
    - `pattern`: glob(s) used when `fs_discover` is true.
    - `fs_discover`: when true, expand `pattern` into module names and
      add them to `modules` before importing.
    """
    if fs_discover:
        if isinstance(pattern, str):
            pattern = (pattern,)
        discovered_modules = set()
        for glob_pattern in pattern:
            for path in Path().glob(glob_pattern):
                if path.is_file():
                    if path.suffix in (".py", ".pyc", ".pyd", ".so"):
                        # remove all suffixes
                        prefix = path.name.partition(".")[0]
                        discovered_modules.add(
                            str(path.with_name(prefix)).replace(os.path.sep, ".")
                        )
                    # ignore other files
                else:
                    discovered_modules.add(str(path).replace(os.path.sep, "."))

        modules.extend(list(discovered_modules))
    import_from_modules(modules)


def object_fqn(obj: object) -> str:
    if hasattr(obj, "__name__"):
        return f"{obj.__module__}.{obj.__name__}"
    return f"{obj.__module__}.{obj.__class__.__name__}"


def get_type_fqn(arg: Any) -> str | None:
    _resolved_module = ""
    try:
        _resolved_module = arg.__module__
    except AttributeError:
        if arg.__class__.__name__ in __builtins__:
            _resolved_module = "builtins"

    if _resolved_module == "":
        return None

    return _resolved_module + ":" + arg.__class__.__name__


def get_type_from_fqn(_result: str | bytes | None) -> Any:
    _imported_type = None
    if _result is None:
        return _imported_type

    _decoded_result = _result.decode() if isinstance(_result, bytes) else _result
    try:
        _imported_type = import_object(_decoded_result)
    except Exception as exc:
        logger.warning("{}", exc)

    return _imported_type
