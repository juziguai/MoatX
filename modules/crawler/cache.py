"""Disk JSON cache for crawler data."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, time, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .models import (
    CACHE_EXPIRED,
    CACHE_MISS,
    CACHE_VERSION_MISMATCH,
    CrawlResult,
    PARSE_ERROR,
)


CURRENT_CACHE_VERSION = 1
CN_TZ = timezone(timedelta(hours=8))
_CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / ".cache"


def beijing_now() -> datetime:
    return datetime.now(CN_TZ)


def cache_dir() -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR


def _safe_key(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", key).strip("_")


def cache_path(key: str) -> Path:
    return cache_dir() / f"{_safe_key(key)}.json"


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CN_TZ)
    return parsed.astimezone(CN_TZ)


def _crossed_key_session_time(cached: datetime, now: datetime) -> bool:
    cached_t = cached.time()
    now_t = now.time()
    return (
        cached.date() == now.date()
        and (
            (cached_t < time(9, 30) <= now_t)
            or (time(11, 30) <= cached_t < time(13, 0) <= now_t)
        )
    )


def is_cache_stale(cached_at: str, max_age_seconds: int | None = None) -> bool:
    cached = _parse_datetime(cached_at)
    if cached is None:
        return True

    now = beijing_now()
    if _crossed_key_session_time(cached, now):
        return True

    if max_age_seconds is None:
        return False

    return (now - cached).total_seconds() > max_age_seconds


def build_cache_key(prefix: str, trade_date: str, intraday_hhmm: str = "") -> str:
    date_part = trade_date.replace("-", "")
    return f"{prefix}_{date_part}_{intraday_hhmm}" if intraday_hhmm else f"{prefix}_{date_part}"


def write_json_cache(key: str, data: Any, source: str, trade_date: str = "") -> str:
    cached_at = beijing_now().isoformat(timespec="seconds")
    payload = {
        "_cache_version": CURRENT_CACHE_VERSION,
        "_cached_at": cached_at,
        "_source": source,
        "_trade_date": trade_date,
        "data": data,
    }
    path = cache_path(key)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)
    return str(path)


def read_json_cache(key: str, max_age_seconds: int | None = None) -> CrawlResult:
    path = cache_path(key)
    if not path.exists():
        return CrawlResult(ok=False, error=CACHE_MISS, user_message="缓存不存在")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return CrawlResult(
            ok=False,
            error=PARSE_ERROR,
            error_detail=str(exc),
            user_message="缓存文件解析失败",
        )

    cached_at = payload.get("_cached_at", "")
    source = payload.get("_source", "")
    trade_date = payload.get("_trade_date", "")

    if payload.get("_cache_version") != CURRENT_CACHE_VERSION:
        return CrawlResult(
            ok=False,
            error=CACHE_VERSION_MISMATCH,
            source=source,
            from_cache=True,
            cached_at=cached_at,
            trade_date=trade_date,
            data=payload.get("data"),
            user_message="缓存版本不匹配",
        )

    if is_cache_stale(cached_at, max_age_seconds=max_age_seconds):
        return CrawlResult(
            ok=False,
            error=CACHE_EXPIRED,
            source=source,
            from_cache=True,
            cached_at=cached_at,
            trade_date=trade_date,
            data=payload.get("data"),
            user_message="缓存已过期",
        )

    return CrawlResult(
        ok=True,
        data=payload.get("data"),
        source=source,
        from_cache=True,
        cached_at=cached_at,
        trade_date=trade_date,
    )


def _df_cache_path(key: str) -> Path:
    """Return the path for a DataFrame-based cache file (.parquet)."""
    return cache_dir() / f"{_safe_key(key)}.parquet"


def write_df_cache(key: str, df: "pd.DataFrame", source: str, trade_date: str = "") -> str:
    """Write a DataFrame to a Parquet cache file with embedded metadata.

    Metadata (version, cached_at, source, trade_date) is stored in the
    Parquet file's schema metadata, so a single file holds everything.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    cached_at = beijing_now().isoformat(timespec="seconds")
    metadata = {
        "_cache_version": str(CURRENT_CACHE_VERSION),
        "_cached_at": cached_at,
        "_source": source,
        "_trade_date": trade_date,
        "_pandas_version": pd.__version__,
    }
    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata(metadata)
    path = _df_cache_path(key)
    pq.write_table(table, path, compression="zstd")
    return str(path)


def read_df_cache(key: str, max_age_seconds: int | None = None) -> CrawlResult:
    """Read a DataFrame from a Parquet cache file.

    Returns CrawlResult with data=pd.DataFrame on hit,
    or an error CrawlResult with stale data available via .data.
    """
    import pyarrow.parquet as pq

    path = _df_cache_path(key)
    if not path.exists():
        return CrawlResult(ok=False, error=CACHE_MISS, user_message="缓存不存在")

    try:
        table = pq.read_table(path)
    except Exception as exc:
        return CrawlResult(
            ok=False,
            error=PARSE_ERROR,
            error_detail=str(exc),
            user_message="Parquet 缓存文件解析失败",
        )

    raw_meta = table.schema.metadata or {}
    cached_at = raw_meta.get(b"_cached_at", b"").decode() if raw_meta.get(b"_cached_at") else ""
    source = raw_meta.get(b"_source", b"").decode() if raw_meta.get(b"_source") else ""
    trade_date = raw_meta.get(b"_trade_date", b"").decode() if raw_meta.get(b"_trade_date") else ""
    cache_version = raw_meta.get(b"_cache_version", b"").decode() if raw_meta.get(b"_cache_version") else ""

    df = table.to_pandas()

    if cache_version != str(CURRENT_CACHE_VERSION):
        return CrawlResult(
            ok=False,
            error=CACHE_VERSION_MISMATCH,
            source=source,
            from_cache=True,
            cached_at=cached_at,
            trade_date=trade_date,
            data=df,
            user_message="缓存版本不匹配",
        )

    if is_cache_stale(cached_at, max_age_seconds=max_age_seconds):
        return CrawlResult(
            ok=False,
            error=CACHE_EXPIRED,
            source=source,
            from_cache=True,
            cached_at=cached_at,
            trade_date=trade_date,
            data=df,
            user_message="缓存已过期",
        )

    return CrawlResult(
        ok=True,
        data=df,
        source=source,
        from_cache=True,
        cached_at=cached_at,
        trade_date=trade_date,
    )

