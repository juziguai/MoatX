"""Compatibility helpers for optional akshare imports."""

from __future__ import annotations

import logging
import sys
import types
import importlib
import importlib.util
from functools import lru_cache
from pathlib import Path
from typing import Any

_logger = logging.getLogger("moatx.akshare")


class AkshareUnavailable:
    """Placeholder that raises only when an akshare API is actually used."""

    def __init__(self, error: Exception):
        self._error = error

    def __bool__(self) -> bool:
        return False

    @property
    def error(self) -> Exception:
        return self._error

    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(f"akshare unavailable while calling {name}: {self._error}") from self._error


def ensure_py_mini_racer_compat() -> bool:
    """Repair the known mini-racer 0.14 + old py-mini-racer import shape."""
    try:
        import py_mini_racer  # noqa: F401

        return True
    except Exception as exc:
        original_exc = exc

    spec = importlib.util.find_spec("py_mini_racer")
    locations = list(spec.submodule_search_locations or []) if spec else []
    if not locations:
        _logger.debug("py_mini_racer package not found: %s", original_exc)
        return False

    package_dir = Path(locations[0])
    mini_racer_path = package_dir / "_mini_racer.py"
    if not mini_racer_path.exists():
        _logger.debug("py_mini_racer repair skipped; _mini_racer.py missing: %s", original_exc)
        return False

    for name in list(sys.modules):
        if name == "py_mini_racer" or name.startswith("py_mini_racer."):
            sys.modules.pop(name, None)

    package = types.ModuleType("py_mini_racer")
    package.__path__ = [str(package_dir)]
    package.__package__ = "py_mini_racer"
    package.__file__ = str(package_dir / "__init__.py")
    package.__spec__ = spec
    sys.modules["py_mini_racer"] = package

    try:
        mini_racer = importlib.import_module("py_mini_racer._mini_racer")
    except Exception as repair_exc:
        sys.modules.pop("py_mini_racer", None)
        _logger.debug("py_mini_racer repair failed: %s", repair_exc)
        return False

    package.MiniRacer = mini_racer.MiniRacer
    package.StrictMiniRacer = getattr(mini_racer, "StrictMiniRacer", mini_racer.MiniRacer)
    package.mini_racer = getattr(mini_racer, "mini_racer", None)
    package.py_mini_racer = mini_racer
    sys.modules["py_mini_racer.py_mini_racer"] = mini_racer
    _logger.info("repaired py_mini_racer import compatibility for mini-racer")
    return True


@lru_cache(maxsize=1)
def import_akshare() -> Any:
    """Import akshare with graceful degradation for incompatible runtimes."""
    try:
        ensure_py_mini_racer_compat()
        import akshare as ak

        return ak
    except Exception as exc:
        _logger.warning("akshare import failed; akshare-backed data will degrade: %s", exc)
        return AkshareUnavailable(exc)
