"""THS fund flow page JS anti-crawling — standalone replacement for akshare's internal module.

Bundles ths.js locally so we don't depend on akshare.stock_feature.stock_fund_flow,
which is an internal module that breaks when akshare updates.
"""

from __future__ import annotations

from pathlib import Path

import py_mini_racer

_JS_DIR = Path(__file__).resolve().parent / "js"
_JS_PATH = _JS_DIR / "ths.js"
_CTX: py_mini_racer.MiniRacer | None = None


def _get_ctx() -> py_mini_racer.MiniRacer:
    global _CTX
    if _CTX is None:
        _CTX = py_mini_racer.MiniRacer()
        _CTX.eval(_JS_PATH.read_text(encoding="utf-8"))
    return _CTX


def get_hexin_v_header() -> str:
    """Generate the hexin-v header value required by THS fund flow pages."""
    ctx = _get_ctx()
    return ctx.call("v")
