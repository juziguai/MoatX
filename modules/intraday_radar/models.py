"""Data models for intraday radar signals."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class RadarConfig:
    min_score: float = 65.0
    min_pct: float = 3.0
    max_entry_pct: float = 7.8
    min_ret_10m: float = 2.0
    min_amount_ratio: float = 1.8
    scan_minutes: int = 10
    morning_cutoff: str = "11:30"
    enable_sector_resonance: bool = True
    sector_resonance_min_signals: int = 2
    sector_resonance_boost_cap: float = 22.0


@dataclass(slots=True)
class MinuteBar:
    time: str
    price: float
    cum_volume: float
    cum_amount: float
    minute_volume: float = 0.0
    minute_amount: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RadarSignal:
    symbol: str
    name: str
    signal_time: str
    price: float
    pct_change: float
    score: float
    level: str
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
