"""Universe construction for intraday radar scans."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from modules.market_filters import is_excluded_selection_board
from modules.utils import normalize_symbol


def parse_symbols(value: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in str(value or "").replace("，", ",").replace("\n", ",").split(","):
        code = normalize_symbol(raw.strip())
        if code.isdigit():
            code = code.zfill(6)
        if not code or code in seen or is_excluded_selection_board(code):
            continue
        seen.add(code)
        out.append(code)
    return out


def load_symbols_file(path: str | Path) -> list[str]:
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(str(source))
    text = source.read_text(encoding="utf-8-sig").strip()
    if not text:
        return []
    if source.suffix.lower() == ".json" or text[:1] in {"[", "{"}:
        data: Any = json.loads(text)
        if isinstance(data, dict):
            for key in ("symbols", "universe", "stocks", "codes"):
                if key in data:
                    data = data[key]
                    break
        values: list[str] = []
        if isinstance(data, list):
            for item in data:
                values.extend(_symbols_from_item(item))
        else:
            values.extend(_symbols_from_item(data))
        return parse_symbols(",".join(values))
    return parse_symbols(text)


def _symbols_from_item(item: Any) -> list[str]:
    if isinstance(item, str):
        return parse_symbols(item)
    if isinstance(item, dict):
        for key in ("code", "symbol", "ts_code", "stock_code"):
            if item.get(key):
                return parse_symbols(str(item[key]))
    return []
