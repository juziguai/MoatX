"""Config-backed stock-topic exposure table for event scoring."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from modules.config import tomllib
from modules.sector_tags import SectorTagProvider

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_EXPOSURE_PATH = _PROJECT_ROOT / "data" / "stock_topic_exposure.toml"


@dataclass(slots=True)
class StockTopicExposure:
    symbol: str
    topic: str
    sector_tag: str = ""
    stock_name: str = ""
    exposure: float = 1.0
    confidence: float = 1.0
    source: str = "manual"
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StockTopicExposureProvider:
    """Read curated stock-topic exposure weights from a small TOML table."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_EXPOSURE_PATH
        self._rows: list[StockTopicExposure] | None = None

    def rows(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        wanted = {SectorTagProvider.normalize_code(symbol) for symbol in symbols or []}
        rows = [
            row.to_dict()
            for row in self._load()
            if not wanted or row.symbol in wanted
        ]
        rows.sort(key=lambda row: (row["symbol"], row["topic"], -float(row["exposure"])))
        return rows

    def match(
        self,
        symbol: str,
        *,
        topic: str = "",
        event_tag: str = "",
        stock_tag: str = "",
    ) -> dict[str, Any] | None:
        code = SectorTagProvider.normalize_code(symbol)
        matches = [
            row
            for row in self._load()
            if row.symbol == code
            and self._topic_matches(row.topic, topic)
            and self._tag_matches(row.sector_tag, event_tag, stock_tag)
        ]
        if not matches:
            return None
        best = max(matches, key=lambda row: row.exposure * row.confidence)
        return best.to_dict()

    def weight(
        self,
        symbol: str,
        *,
        topic: str = "",
        event_tag: str = "",
        stock_tag: str = "",
        fallback: float = 0.0,
    ) -> tuple[float, dict[str, Any] | None]:
        row = self.match(symbol, topic=topic, event_tag=event_tag, stock_tag=stock_tag)
        if row is None:
            return fallback, None
        exposure = self._clamp(float(row.get("exposure") or 0.0), 0.0, 1.2)
        confidence = self._clamp(float(row.get("confidence") or 1.0), 0.0, 1.0)
        return round(exposure * confidence, 4), row

    def _load(self) -> list[StockTopicExposure]:
        if self._rows is not None:
            return self._rows
        if not self._path.exists():
            self._rows = []
            return self._rows
        raw = tomllib.loads(self._path.read_text(encoding="utf-8"))
        rows: list[StockTopicExposure] = []
        for item in raw.get("exposures", []):
            symbol = SectorTagProvider.normalize_code(str(item.get("symbol") or ""))
            topic = str(item.get("topic") or "").strip()
            if not symbol or not topic:
                continue
            rows.append(
                StockTopicExposure(
                    symbol=symbol,
                    topic=topic,
                    sector_tag=str(item.get("sector_tag") or "").strip(),
                    stock_name=str(item.get("stock_name") or item.get("name") or "").strip(),
                    exposure=self._clamp(float(item.get("exposure") or 1.0), 0.0, 1.2),
                    confidence=self._clamp(float(item.get("confidence") or 1.0), 0.0, 1.0),
                    source=str(item.get("source") or "manual").strip(),
                    updated_at=str(item.get("updated_at") or "").strip(),
                )
            )
        self._rows = rows
        return self._rows

    @staticmethod
    def _topic_matches(config_topic: str, runtime_topic: str) -> bool:
        if not runtime_topic:
            return True
        return str(config_topic or "").strip() == str(runtime_topic or "").strip()

    @staticmethod
    def _tag_matches(config_tag: str, event_tag: str, stock_tag: str) -> bool:
        if not config_tag:
            return True
        if event_tag and SectorTagProvider.tag_matches(config_tag, event_tag):
            return True
        return bool(stock_tag and SectorTagProvider.tag_matches(config_tag, stock_tag))

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))
