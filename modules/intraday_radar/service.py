"""Intraday radar service orchestration."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from modules.stock_data import StockData
from modules.utils import normalize_symbol

from .data import TencentMinuteClient
from .detector import LaunchDetector
from .models import RadarConfig
from .sector import SectorResonanceScorer
from .storage import RadarStorage
from .universe import load_symbols_file, parse_symbols


class IntradayRadarService:
    def __init__(
        self,
        *,
        config: RadarConfig | None = None,
        minute_client: TencentMinuteClient | None = None,
        storage: RadarStorage | None = None,
    ) -> None:
        self.config = config or RadarConfig()
        self.minute_client = minute_client or TencentMinuteClient()
        self.detector = LaunchDetector(self.config)
        self.sector_scorer = SectorResonanceScorer(self.config)
        self.storage = storage or RadarStorage()
        self._sd = StockData()

    def scan(
        self,
        *,
        symbols: list[str],
        trade_date: str | None = None,
        write_snapshot: bool = False,
    ) -> dict[str, Any]:
        started = datetime.now()
        names = self._quote_names(symbols)
        rows = []
        errors = []
        for symbol in symbols:
            code = normalize_symbol(symbol)
            if code.isdigit():
                code = code.zfill(6)
            if not code:
                continue
            try:
                result = self.replay(symbol=code, trade_date=trade_date, name=names.get(code, ""))
                rows.append(result)
            except Exception as exc:
                errors.append({"symbol": code, "error": str(exc)})
        signals = []
        for row in rows:
            signals.extend(row.get("signals") or [])
        sector_resonance = []
        if self.config.enable_sector_resonance:
            sector_resonance = self.sector_scorer.apply(results=rows, signals=signals)
        signals.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
        payload = {
            "engine": "intraday_radar_v1",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "trade_date": trade_date or "",
            "requested": len(symbols),
            "scanned": len(rows),
            "signal_count": len(signals),
            "signals": signals,
            "sector_resonance": sector_resonance,
            "results": rows,
            "errors": errors,
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }
        if write_snapshot:
            path = self.storage.write_snapshot(payload, prefix="radar")
            payload["snapshot_path"] = str(path)
        return payload

    def replay(self, *, symbol: str, trade_date: str | None = None, name: str = "") -> dict[str, Any]:
        code = normalize_symbol(symbol)
        minute_df, meta = self.minute_client.fetch_day(code, trade_date=trade_date)
        actual_date = str(meta.get("trade_date") or trade_date or "")
        prev_close = self._prev_close(code, actual_date)
        resolved_name = name or self._name_from_daily(code, actual_date) or str(meta.get("name") or code)
        detected = self.detector.detect(minute_df, symbol=code, name=resolved_name, prev_close=prev_close)
        return {
            "symbol": code,
            "name": resolved_name,
            "trade_date": actual_date,
            "prev_close": round(prev_close, 3),
            "source": meta,
            **detected,
        }

    @staticmethod
    def resolve_symbols(symbols: str = "", symbols_file: str = "") -> list[str]:
        out = parse_symbols(symbols)
        if symbols_file:
            out.extend(load_symbols_file(symbols_file))
        return parse_symbols(",".join(out))

    def _prev_close(self, symbol: str, trade_date: str) -> float:
        daily = self._daily(symbol, trade_date)
        if daily.empty:
            return 0.0
        target = pd.Timestamp(trade_date)
        prior = daily[pd.to_datetime(daily.index).normalize() < target.normalize()]
        if prior.empty:
            return 0.0
        return float(prior.iloc[-1].get("close") or 0.0)

    def _quote_names(self, symbols: list[str]) -> dict[str, str]:
        codes = [normalize_symbol(symbol).zfill(6) for symbol in symbols if normalize_symbol(symbol)]
        if not codes:
            return {}
        try:
            quotes = self._sd.get_realtime_quotes(codes)
            out: dict[str, str] = {}
            for key, quote in (quotes or {}).items():
                code = normalize_symbol(str(quote.get("code") or key)).zfill(6)
                name = str(quote.get("name") or "")
                if code and name:
                    out[code] = name
            return out
        except Exception:
            return {}

    def _name_from_daily(self, symbol: str, trade_date: str) -> str:
        try:
            spot = self._sd.get_spot()
            if spot is not None and not spot.empty and "code" in spot.columns:
                match = spot[spot["code"].map(normalize_symbol) == normalize_symbol(symbol)]
                if not match.empty:
                    return str(match.iloc[0].get("name") or "")
        except Exception:
            pass
        return ""

    def _daily(self, symbol: str, trade_date: str) -> pd.DataFrame:
        end = "".join(ch for ch in str(trade_date) if ch.isdigit())[:8]
        start = "20250101"
        df = self._sd.get_daily(symbol, start_date=start, end_date=end)
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out.dropna(subset=["date"]).set_index("date")
        for column in ("open", "high", "low", "close", "volume", "amount"):
            if column in out.columns:
                out[column] = pd.to_numeric(out[column], errors="coerce")
        return out.dropna(subset=["close"]).sort_index()
