"""Unified Result type for all data source operations.

Replaces CrawlResult + raw dict/list returns with a single generic type.
"""

from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class Result(Generic[T]):
    """Generic result from any data source operation.

    Usage:
        Result.ok(data=df, source="sina")
        Result.fail(error="timeout", source="sina")
    """

    ok: bool
    data: T | None = None
    source: str = ""
    error: str = ""
    error_detail: str = ""
    warnings: list[str] = field(default_factory=list)
    elapsed_ms: float = 0.0
    from_cache: bool = False
    trade_date: str = ""

    @classmethod
    def ok(cls, data: T, source: str = "", **kwargs) -> "Result[T]":
        return cls(ok=True, data=data, source=source, **kwargs)

    @classmethod
    def fail(cls, error: str, source: str = "", **kwargs) -> "Result[T]":
        return cls(ok=False, data=None, error=error, source=source, **kwargs)

    def unwrap(self) -> T:
        if not self.ok:
            raise ValueError(f"Result error [{self.source}]: {self.error}")
        return self.data  # type: ignore[return-value]

    @property
    def empty(self) -> bool:
        return self.data is None
