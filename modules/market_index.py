"""Multi-source A-share market index quotes."""

from __future__ import annotations

import concurrent.futures
from dataclasses import asdict, dataclass, field
from datetime import datetime
import math
import re
import time
from typing import Any

import requests

from modules.config import cfg


DEFAULT_INDEX_CODES = ["sh000001", "sz399001", "sz399006", "sh000300", "sh000688", "bj899050"]
INDEX_ALIASES = {
    "000001": "sh000001",
    "sh000001": "sh000001",
    "上证": "sh000001",
    "上证指数": "sh000001",
    "399001": "sz399001",
    "sz399001": "sz399001",
    "深证": "sz399001",
    "深成指": "sz399001",
    "深证成指": "sz399001",
    "399006": "sz399006",
    "sz399006": "sz399006",
    "创业板": "sz399006",
    "创业板指": "sz399006",
    "000300": "sh000300",
    "sh000300": "sh000300",
    "沪深300": "sh000300",
    "300": "sh000300",
    "000688": "sh000688",
    "sh000688": "sh000688",
    "科创50": "sh000688",
    "899050": "bj899050",
    "bj899050": "bj899050",
    "北证50": "bj899050",
}


@dataclass(slots=True)
class IndexQuote:
    """One normalized market index quote."""

    code: str
    name: str
    price: float
    prev_close: float
    change: float
    pct_change: float
    volume: float = 0.0
    amount: float = 0.0
    datetime: str = ""
    source: str = ""


@dataclass(slots=True)
class AggregatedIndexQuote:
    """Validated quote merged from one or more sources."""

    code: str
    name: str
    price: float
    prev_close: float
    change: float
    pct_change: float
    datetime: str
    status: str
    sources: list[str] = field(default_factory=list)
    source_quotes: list[dict[str, Any]] = field(default_factory=list)
    max_price_diff: float = 0.0
    max_pct_diff: float = 0.0
    warning: str = ""


@dataclass(slots=True)
class MarketBreadth:
    """A-share market breadth snapshot."""

    total: int
    up: int
    down: int
    flat: int
    datetime: str
    source: str
    method: str
    elapsed: float = 0.0
    warning: str = ""


class MarketIndexQuoteManager:
    """Fetch A-share market indices from Tencent and Sina, then validate and aggregate."""

    def __init__(self, tolerance_pct: float = 0.08, timeout: int | None = None):
        self.tolerance_pct = tolerance_pct
        self.timeout = timeout or cfg().crawler.timeout

    def fetch(
        self,
        codes: list[str] | None = None,
        sources: list[str] | None = None,
    ) -> list[AggregatedIndexQuote]:
        normalized_codes = normalize_index_codes(codes or DEFAULT_INDEX_CODES)
        source_names = [s.lower() for s in (sources or ["tencent", "sina"])]
        source_results: dict[str, dict[str, IndexQuote]] = {}
        if "tencent" in source_names:
            source_results["tencent"] = fetch_tencent_indices(normalized_codes, timeout=self.timeout)
        if "sina" in source_names:
            source_results["sina"] = fetch_sina_indices(normalized_codes, timeout=self.timeout)

        rows: list[AggregatedIndexQuote] = []
        for code in normalized_codes:
            quotes = [result[code] for result in source_results.values() if code in result]
            if not quotes:
                continue
            rows.append(self._aggregate(code, quotes))
        return rows

    def _aggregate(self, code: str, quotes: list[IndexQuote]) -> AggregatedIndexQuote:
        quotes = sorted(quotes, key=lambda q: (q.datetime, q.source), reverse=True)
        primary = quotes[0]
        max_price_diff = 0.0
        max_pct_diff = 0.0
        for quote in quotes[1:]:
            max_price_diff = max(max_price_diff, abs(primary.price - quote.price))
            max_pct_diff = max(max_pct_diff, abs(primary.pct_change - quote.pct_change))

        status = "single_source" if len(quotes) == 1 else "verified"
        warning = ""
        if len(quotes) >= 2 and max_pct_diff > self.tolerance_pct:
            status = "diverged"
            warning = f"多源涨跌幅差异 {max_pct_diff:.2f}%，超过阈值 {self.tolerance_pct:.2f}%"

        return AggregatedIndexQuote(
            code=code,
            name=primary.name,
            price=primary.price,
            prev_close=primary.prev_close,
            change=primary.change,
            pct_change=primary.pct_change,
            datetime=primary.datetime,
            status=status,
            sources=[quote.source for quote in quotes],
            source_quotes=[asdict(quote) for quote in quotes],
            max_price_diff=round(max_price_diff, 4),
            max_pct_diff=round(max_pct_diff, 4),
            warning=warning,
        )

    def breadth(self, source: str = "sina") -> MarketBreadth:
        """Return market breadth counts using the fastest available source."""
        if source.lower() != "sina":
            raise ValueError("当前仅支持新浪市场宽度快照")
        return fetch_sina_market_breadth(timeout=self.timeout)


def normalize_index_codes(codes: list[str]) -> list[str]:
    """Normalize user input to provider index codes such as sh000001."""
    normalized: list[str] = []
    for raw in codes:
        key = str(raw or "").strip()
        if not key:
            continue
        lowered = key.lower()
        code = INDEX_ALIASES.get(key) or INDEX_ALIASES.get(lowered)
        if not code:
            digits = re.sub(r"\D", "", key)
            code = INDEX_ALIASES.get(digits, lowered)
        if code not in normalized:
            normalized.append(code)
    return normalized or list(DEFAULT_INDEX_CODES)


def fetch_tencent_indices(codes: list[str], timeout: int | None = None) -> dict[str, IndexQuote]:
    """Fetch index quotes from Tencent qt.gtimg.cn."""
    session = _session()
    resp = session.get(
        "http://qt.gtimg.cn/q",
        params={"q": ",".join(codes)},
        timeout=timeout or cfg().crawler.timeout,
    )
    resp.encoding = "gbk"
    resp.raise_for_status()

    rows: dict[str, IndexQuote] = {}
    for match in re.finditer(r'v_([a-z]{2}\d+)="([^"]*)"', resp.text):
        raw_code = match.group(1)
        parts = match.group(2).split("~")
        if len(parts) < 38:
            continue
        quote = _tencent_quote(raw_code, parts)
        if quote is not None:
            rows[raw_code] = quote
    return rows


def fetch_sina_indices(codes: list[str], timeout: int | None = None) -> dict[str, IndexQuote]:
    """Fetch index quotes from Sina hq.sinajs.cn."""
    session = _session()
    resp = session.get(
        "https://hq.sinajs.cn/list=" + ",".join(codes),
        timeout=timeout or cfg().crawler.timeout,
        headers={"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"},
    )
    resp.encoding = "gbk"
    resp.raise_for_status()

    rows: dict[str, IndexQuote] = {}
    for line in resp.text.strip().splitlines():
        match = re.match(r'var hq_str_([a-z]{2}\d+)="(.*)";', line.strip())
        if not match:
            continue
        raw_code = match.group(1)
        parts = match.group(2).split(",")
        quote = _sina_quote(raw_code, parts)
        if quote is not None:
            rows[raw_code] = quote
    return rows


def fetch_sina_market_breadth(timeout: int | None = None, page_size: int = 100) -> MarketBreadth:
    """Fetch market breadth via Sina total-count endpoint plus sorted boundary pages."""
    started = time.perf_counter()
    total = _sina_stock_count(timeout=timeout)
    if total <= 0:
        raise RuntimeError("新浪市场总数接口返回为空")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            up_future = executor.submit(_sina_count_by_boundary, total, True, timeout, page_size)
            down_future = executor.submit(_sina_count_by_boundary, total, False, timeout, page_size)
            up = up_future.result()
            down = down_future.result()
        flat = total - up - down
        if up < 0 or down < 0 or flat < 0:
            raise RuntimeError(f"invalid breadth counts: total={total}, up={up}, down={down}, flat={flat}")
    except Exception as exc:
        fallback = _sina_market_breadth_full_scan(
            total_hint=total,
            timeout=timeout,
            page_size=page_size,
            started=started,
        )
        fallback.warning = f"边界快照异常，已回退全量扫描: {exc}"
        return fallback
    return MarketBreadth(
        total=total,
        up=up,
        down=down,
        flat=flat,
        datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        source="sina",
        method="stock_count+sorted_boundary",
        elapsed=round(time.perf_counter() - started, 3),
    )


def _sina_stock_count(timeout: int | None = None) -> int:
    session = _session()
    resp = session.get(
        "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount",
        params={"node": "hs_a"},
        timeout=timeout or cfg().crawler.timeout,
        headers={"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()
    return int(str(resp.text).strip().strip('"'))


def _sina_count_by_boundary(
    total: int,
    positive: bool,
    timeout: int | None = None,
    page_size: int = 100,
) -> int:
    pages = math.ceil(total / page_size)
    lo, hi = 1, pages
    boundary = pages + 1
    asc = not positive

    while lo <= hi:
        mid = (lo + hi) // 2
        pcts = _sina_sorted_pct_page(mid, asc=asc, timeout=timeout, page_size=page_size)
        if positive:
            if min(pcts) > 0:
                lo = mid + 1
            else:
                boundary = mid
                hi = mid - 1
        else:
            if max(pcts) < 0:
                lo = mid + 1
            else:
                boundary = mid
                hi = mid - 1

    before = (boundary - 1) * page_size
    if boundary > pages:
        return min(before, total)
    pcts = _sina_sorted_pct_page(boundary, asc=asc, timeout=timeout, page_size=page_size)
    if positive:
        count = before + sum(1 for pct in pcts if pct > 0)
    else:
        count = before + sum(1 for pct in pcts if pct < 0)
    return min(count, total)


def _sina_sorted_pct_page(
    page: int,
    asc: bool,
    timeout: int | None = None,
    page_size: int = 100,
) -> list[float]:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            rows = _sina_sorted_rows_page(page, asc=asc, timeout=timeout, page_size=page_size)
            pcts: list[float] = []
            for row in rows:
                try:
                    pcts.append(float(row.get("changepercent")))
                except (TypeError, ValueError):
                    continue
            if pcts:
                return pcts
            last_error = RuntimeError(f"empty pct page: page={page}, asc={asc}")
        except Exception as exc:
            last_error = exc
        time.sleep(0.15 * (attempt + 1))
    raise RuntimeError(f"sina sorted pct page unavailable: {last_error}")


def _sina_sorted_rows_page(
    page: int,
    asc: bool,
    timeout: int | None = None,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    session = _session()
    resp = session.get(
        "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
        params={
            "node": "hs_a",
            "num": page_size,
            "page": page,
            "sort": "changepercent",
            "asc": 1 if asc else 0,
            "_s_r_a": "page",
        },
        timeout=timeout or cfg().crawler.timeout,
        headers={"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _sina_market_breadth_full_scan(
    total_hint: int,
    timeout: int | None = None,
    page_size: int = 100,
    started: float | None = None,
) -> MarketBreadth:
    started = started or time.perf_counter()
    pages = max(1, math.ceil(total_hint / page_size))
    rows: list[dict[str, Any]] = []
    max_workers = min(32, pages)

    def fetch_page(page: int) -> list[dict[str, Any]]:
        return _sina_sorted_rows_page(page, asc=False, timeout=timeout, page_size=page_size)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_page, page) for page in range(1, pages + 1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                rows.extend(future.result())
            except Exception:
                continue

    up = down = flat = valid = 0
    for row in rows:
        try:
            pct = float(row.get("changepercent"))
        except (TypeError, ValueError):
            continue
        valid += 1
        if pct > 0:
            up += 1
        elif pct < 0:
            down += 1
        else:
            flat += 1

    if valid <= 0:
        raise RuntimeError("新浪市场宽度全量扫描无有效行情")

    warning = ""
    if total_hint and valid != total_hint:
        warning = f"新浪总数接口 {total_hint}，有效行情 {valid}"

    return MarketBreadth(
        total=valid,
        up=up,
        down=down,
        flat=flat,
        datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        source="sina",
        method="full_scan",
        elapsed=round(time.perf_counter() - started, 3),
        warning=warning,
    )


def _tencent_quote(raw_code: str, parts: list[str]) -> IndexQuote | None:
    name = parts[1] if len(parts) > 1 else ""
    price = _float_at(parts, 3)
    prev_close = _float_at(parts, 4)
    change = _float_at(parts, 31)
    pct_change = _float_at(parts, 32)
    if not name or price is None or prev_close is None:
        return None
    if change is None:
        change = price - prev_close
    if pct_change is None:
        pct_change = change / prev_close * 100 if prev_close else 0.0
    return IndexQuote(
        code=raw_code,
        name=name,
        price=float(price),
        prev_close=float(prev_close),
        change=float(change),
        pct_change=float(pct_change),
        volume=float(_float_at(parts, 6) or 0.0),
        amount=float(_float_at(parts, 37) or 0.0),
        datetime=_format_provider_datetime(parts[30] if len(parts) > 30 else ""),
        source="tencent",
    )


def _sina_quote(raw_code: str, parts: list[str]) -> IndexQuote | None:
    if len(parts) < 4 or not parts[0]:
        return None
    price = _float_at(parts, 3)
    prev_close = _float_at(parts, 2)
    if price is None or prev_close is None:
        return None
    change = price - prev_close
    pct_change = change / prev_close * 100 if prev_close else 0.0
    timestamp = ""
    if len(parts) > 31 and parts[30] and parts[31]:
        timestamp = f"{parts[30]} {parts[31]}"
    return IndexQuote(
        code=raw_code,
        name=parts[0],
        price=float(price),
        prev_close=float(prev_close),
        change=float(change),
        pct_change=float(pct_change),
        volume=float(_float_at(parts, 8) or 0.0),
        amount=float(_float_at(parts, 9) or 0.0),
        datetime=timestamp,
        source="sina",
    )


def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    return session


def _float_at(parts: list[str], index: int) -> float | None:
    try:
        value = parts[index]
    except IndexError:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_provider_datetime(value: str) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{14}", text):
        return datetime.strptime(text, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    return text
