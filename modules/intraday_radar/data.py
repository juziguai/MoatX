"""Minute quote data access for intraday radar."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import requests

from modules.utils import normalize_symbol


class TencentMinuteClient:
    """Fetch Tencent minute-level quote data.

    The `day/query` endpoint returns recent trading days and is useful for
    replaying the latest available session. Values are cumulative by minute.
    """

    _BASE = "https://web.ifzq.gtimg.cn/appstock/app/day/query"

    def fetch_day(self, symbol: str, *, trade_date: str | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        code = normalize_symbol(symbol)
        market = "sh" if code.startswith(("5", "6", "9")) else "sz"
        tc_code = f"{market}{code}"
        session = requests.Session()
        session.trust_env = False
        response = session.get(
            self._BASE,
            params={"code": tc_code},
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://gu.qq.com/",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        stock = ((payload.get("data") or {}).get(tc_code) or {})
        days = stock.get("data") or []
        if not days:
            return pd.DataFrame(), {"source": "tencent", "status": "empty", "symbol": code}

        wanted = _date_compact(trade_date) if trade_date else ""
        day = next((item for item in days if str(item.get("date")) == wanted), None)
        if day is None:
            day = days[-1]
        date_text = str(day.get("date") or "")
        rows = []
        for item in day.get("data") or []:
            parts = str(item).split()
            if len(parts) < 4:
                continue
            hhmm = parts[0]
            try:
                price = float(parts[1])
                cum_volume = float(parts[2])
                cum_amount = float(parts[3])
            except ValueError:
                continue
            if not date_text or len(hhmm) != 4:
                continue
            rows.append(
                {
                    "time": pd.Timestamp(f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]} {hhmm[:2]}:{hhmm[2:]}"),
                    "hhmm": hhmm,
                    "price": price,
                    "cum_volume": cum_volume,
                    "cum_amount": cum_amount,
                }
            )
        df = pd.DataFrame(rows)
        if df.empty:
            return df, {"source": "tencent", "status": "empty_day", "symbol": code, "trade_date": date_text}
        df = df.sort_values("time").reset_index(drop=True)
        df["minute_volume"] = df["cum_volume"].diff().fillna(df["cum_volume"]).clip(lower=0)
        df["minute_amount"] = df["cum_amount"].diff().fillna(df["cum_amount"]).clip(lower=0)
        return df, {
            "source": "tencent",
            "status": "ok",
            "symbol": code,
            "trade_date": _date_display(date_text),
            "name": stock.get("qt", {}).get(code, {}).get("name", ""),
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
        }


def _date_compact(value: str | None) -> str:
    text = str(value or "").strip()
    return "".join(ch for ch in text if ch.isdigit())[:8]


def _date_display(value: str) -> str:
    text = _date_compact(value)
    if len(text) == 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text
