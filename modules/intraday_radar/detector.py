"""Intraday launch pattern detection."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .models import RadarConfig, RadarSignal


class LaunchDetector:
    """Detect afternoon launch / board-sweep candidates from minute bars."""

    def __init__(self, config: RadarConfig | None = None) -> None:
        self.config = config or RadarConfig()

    def detect(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
        name: str = "",
        prev_close: float = 0.0,
    ) -> dict[str, Any]:
        if df is None or df.empty or prev_close <= 0:
            return {"signals": [], "status": "no_data"}
        data = df.copy().sort_values("time").reset_index(drop=True)
        data["pct_change"] = (pd.to_numeric(data["price"], errors="coerce") / prev_close - 1.0) * 100
        data["ret_3m"] = data["price"].pct_change(3) * 100
        data["ret_5m"] = data["price"].pct_change(5) * 100
        data["ret_10m"] = data["price"].pct_change(self.config.scan_minutes) * 100
        avg_amount = data["minute_amount"].rolling(20, min_periods=5).mean().shift(1)
        data["amount_ratio"] = data["minute_amount"] / avg_amount.replace(0, pd.NA)
        data["limit_price"] = round(prev_close * 1.1 + 1e-8, 2)
        data["distance_to_limit_pct"] = (data["limit_price"] / data["price"] - 1.0) * 100

        morning = data[data["time"].dt.strftime("%H:%M") <= self.config.morning_cutoff]
        morning_high = float(morning["price"].max()) if not morning.empty else float(data["price"].iloc[0])
        morning_low = float(morning["price"].min()) if not morning.empty else float(data["price"].iloc[0])
        signals: list[RadarSignal] = []
        for _, row in data.iterrows():
            time_text = row["time"].strftime("%H:%M")
            if time_text < "10:00":
                continue
            score, reasons, warnings = self._score_row(row, morning_high=morning_high)
            if score < self.config.min_score:
                continue
            signal = RadarSignal(
                symbol=symbol,
                name=name or symbol,
                signal_time=time_text,
                price=round(float(row["price"]), 3),
                pct_change=round(float(row["pct_change"]), 3),
                score=round(score, 1),
                level=self._level(score, float(row["pct_change"])),
                reasons=reasons,
                warnings=warnings,
                metrics={
                    "prev_close": round(prev_close, 3),
                    "morning_high": round(morning_high, 3),
                    "morning_low": round(morning_low, 3),
                    "ret_3m": _round(row.get("ret_3m")),
                    "ret_5m": _round(row.get("ret_5m")),
                    "ret_10m": _round(row.get("ret_10m")),
                    "minute_amount": round(float(row.get("minute_amount") or 0.0), 2),
                    "amount_ratio": _round(row.get("amount_ratio")),
                    "distance_to_limit_pct": _round(row.get("distance_to_limit_pct")),
                    "limit_price": round(float(row.get("limit_price") or 0.0), 3),
                },
            )
            signals.append(signal)
            break

        first_limit = data[data["price"] >= data["limit_price"]]
        first_limit_time = ""
        if not first_limit.empty:
            first_limit_time = first_limit.iloc[0]["time"].strftime("%H:%M")
        return {
            "status": "ok",
            "signals": [item.to_dict() for item in signals],
            "summary": {
                "morning_high": round(morning_high, 3),
                "morning_low": round(morning_low, 3),
                "latest_price": round(float(data["price"].iloc[-1]), 3),
                "latest_pct": round(float(data["pct_change"].iloc[-1]), 3),
                "first_limit_time": first_limit_time,
                "minute_count": len(data),
            },
        }

    def _score_row(self, row: pd.Series, *, morning_high: float) -> tuple[float, list[str], list[str]]:
        score = 0.0
        reasons: list[str] = []
        warnings: list[str] = []
        pct = float(row.get("pct_change") or 0.0)
        ret_10m = float(row.get("ret_10m") or 0.0)
        amount_ratio = float(row.get("amount_ratio") or 0.0)
        price = float(row.get("price") or 0.0)
        distance = float(row.get("distance_to_limit_pct") or 0.0)

        if pct >= self.config.min_pct:
            score += 15
            reasons.append(f"涨幅进入异动区间 {pct:+.1f}%")
        if price > morning_high:
            score += 18
            reasons.append(f"突破上午高点 {morning_high:.2f}")
        if ret_10m >= self.config.min_ret_10m:
            score += min(25.0, 12.0 + ret_10m * 4.0)
            reasons.append(f"{self.config.scan_minutes}分钟拉升 {ret_10m:+.1f}%")
        if amount_ratio >= self.config.min_amount_ratio:
            score += min(20.0, 8.0 + amount_ratio * 4.0)
            reasons.append(f"分钟成交额放大 {amount_ratio:.1f}倍")
        if 1.5 <= distance <= 7.0:
            score += 12
            reasons.append(f"距离涨停仍有 {distance:.1f}% 空间")
        elif 0 <= distance < 1.5:
            score += 4
            warnings.append("已接近涨停，追入空间不足")

        if pct > self.config.max_entry_pct:
            score -= 15
            warnings.append(f"涨幅已达 {pct:+.1f}%，进入高追风险区")
        if amount_ratio == 0:
            warnings.append("缺少有效分钟成交额放大确认")
        return score, reasons[:8], warnings[:8]

    @staticmethod
    def _level(score: float, pct: float) -> str:
        if score >= 80 and pct < 8.0:
            return "强异动"
        if score >= 65:
            return "异动观察"
        return "记录"


def _round(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), 3)
    except Exception:
        return 0.0
