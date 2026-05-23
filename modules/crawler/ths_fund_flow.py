"""THS fund flow page JS anti-crawling — standalone replacement for akshare's internal module.

Bundles ths.js locally so we don't depend on akshare.stock_feature.stock_fund_flow,
which is an internal module that breaks when akshare updates.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from modules.akshare_compat import ensure_py_mini_racer_compat

ensure_py_mini_racer_compat()

try:
    from py_mini_racer import MiniRacer
except Exception as exc:  # pragma: no cover - depends on local optional runtime
    MiniRacer = None  # type: ignore[assignment]
    _IMPORT_ERROR: Exception | None = exc
else:
    _IMPORT_ERROR = None

_JS_DIR = Path(__file__).resolve().parent / "js"
_JS_PATH = _JS_DIR / "ths.js"
_CTX: Any | None = None


def _get_ctx() -> Any:
    global _CTX
    if MiniRacer is None:
        raise RuntimeError(f"mini-racer unavailable for THS anti-crawler header: {_IMPORT_ERROR}")
    if _CTX is None:
        _CTX = MiniRacer()
        _CTX.eval(_JS_PATH.read_text(encoding="utf-8"))
    return _CTX


def get_hexin_v_header() -> str:
    """Generate the hexin-v header value required by THS fund flow pages."""
    ctx = _get_ctx()
    return ctx.call("v")
