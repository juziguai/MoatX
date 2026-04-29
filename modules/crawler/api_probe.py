"""Generic API probing and concurrent fetch helpers."""

from __future__ import annotations

import json
import re
import hashlib
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urljoin, urlparse

from requests import Response

from .base import CrawlerClient
from .models import CrawlResult, PARSE_ERROR


SOURCE = "api_probe"
DEFAULT_WORKERS = 8
MAX_PREVIEW_CHARS = 500
MAX_SAMPLE_PATHS = 80
ENDPOINT_PATTERN = re.compile(
    r"""(?P<quote>["'])(?P<url>(?:https?://[^"']+|/[^"'\s<>]+))(?P=quote)"""
)
SCRIPT_SRC_PATTERN = re.compile(r"""<script[^>]+src=["']([^"']+)["']""", re.I)
API_PATH_PATTERN = re.compile(
    r"""(?P<quote>["'])(?P<path>(?:https?:)?//[^"']*?/api/[^"']+|/api/[^"']+|api/[^"']+)(?P=quote)"""
)
STOCK_CODE_PATTERN = re.compile(r"(?<!\d)(?:[036]\d{5}|8\d{5}|9\d{5})(?!\d)")
FIELD_ALIASES = {
    "stock_code": {"code", "symbol", "secucode", "证券代码", "股票代码", "f12", "dm"},
    "stock_name": {"name", "secuname", "证券名称", "股票名称", "名称", "f14", "mc"},
    "price": {"price", "last", "current", "最新价", "现价", "收盘价", "f2"},
    "pct_change": {"pct_change", "changepercent", "涨跌幅", "涨幅", "zdf", "f3"},
    "amount": {"amount", "turnover", "成交额", "成交金额", "f6"},
    "volume": {"volume", "成交量", "f5"},
    "sector": {"sector", "board", "板块", "行业", "概念", "bk_name"},
}
EASTMONEY_FIELD_MAP = {
    "f2": "最新价",
    "f3": "涨跌幅",
    "f4": "涨跌额",
    "f5": "成交量",
    "f6": "成交额",
    "f12": "代码",
    "f13": "市场",
    "f14": "名称",
    "f43": "最新价",
    "f44": "最高价",
    "f45": "最低价",
    "f46": "今开",
    "f47": "成交量",
    "f48": "成交额",
    "f57": "代码",
    "f58": "名称",
    "f60": "昨收",
    "f84": "总股本",
    "f85": "流通股本",
    "f86": "总市值/估值字段",
    "f92": "每股净资产",
    "f116": "总市值",
    "f117": "流通市值",
    "f127": "阶段涨跌幅",
    "f128": "领涨股",
    "f136": "领涨股涨跌幅",
    "f152": "价格精度",
    "f162": "市盈率动态",
    "f163": "市净率",
    "f164": "主力净流入",
    "f167": "主力净占比",
    "f168": "超大单净流入",
    "f169": "涨跌额/超大单净占比",
    "f170": "涨跌幅/大单净流入",
    "f171": "振幅/大单净占比",
    "f172": "中单净流入",
    "f183": "换手率/资金占比",
    "f184": "小单净流入/资金占比",
}
F10_FIELD_MAP = {
    "EPSJB": "基本每股收益",
    "EPSKCJB": "扣非每股收益",
    "BPS": "每股净资产",
    "MGWFPLR": "每股未分配利润",
    "MGJYXJJE": "每股经营现金流",
    "TOTALOPERATEREVE": "营业总收入",
    "TOTALOPERATEREVETZ": "营业总收入同比",
    "PARENTNETPROFIT": "归母净利润",
    "PARENTNETPROFITTZ": "归母净利润同比",
    "KCFJCXSYJLR": "扣非净利润",
    "KCFJCXSYJLRTZ": "扣非净利润同比",
    "XSMLL": "销售毛利率",
    "XSJLL": "销售净利率",
    "ROEJQ": "加权ROE",
    "ZCFZL": "资产负债率",
    "REPORT_DATE_NAME": "报告期",
    "NOTICE_DATE": "公告日期",
}
CHALLENGE_KEYWORDS = {
    "captcha": {
        "captcha",
        "验证码",
        "图形验证",
        "安全验证",
        "verifycode",
        "geetest",
        "滑块",
        "点选",
        "人机验证",
    },
    "risk_control": {
        "访问过于频繁",
        "请求过于频繁",
        "安全风险",
        "风控",
        "异常访问",
        "blocked",
        "forbidden",
        "deny",
        "anti spider",
        "antispider",
        "waf",
    },
    "login_required": {
        "请登录",
        "login required",
        "登录后",
        "未登录",
        "session expired",
        "token expired",
    },
}


@dataclass(frozen=True)
class ApiProbeRequest:
    url: str
    method: str = "GET"
    params: dict[str, Any] | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
    body: str | bytes | dict[str, Any] | None = None
    json_body: dict[str, Any] | list[Any] | None = None
    source: str = SOURCE


@dataclass
class ApiProbeResult:
    url: str
    ok: bool
    status_code: int | None = None
    content_type: str = ""
    response_kind: str = ""
    elapsed_ms: int = 0
    error: str = ""
    error_detail: str = ""
    preview: str = ""
    json_keys: list[str] = field(default_factory=list)
    item_count: int | None = None
    discovered_urls: list[str] = field(default_factory=list)
    score: int = 0
    score_reasons: list[str] = field(default_factory=list)
    stock_fields: list[str] = field(default_factory=list)
    sample_paths: list[str] = field(default_factory=list)
    api_hint: bool = False
    challenge_detected: bool = False
    challenge_type: str = ""
    challenge_reasons: list[str] = field(default_factory=list)
    api_vendor: str = ""
    api_category: str = ""
    api_module: str = ""
    semantic_fields: dict[str, str] = field(default_factory=dict)
    semantic_summary: str = ""


def probe_url(
    url: str,
    timeout: int = 8,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    body: str | bytes | dict[str, Any] | None = None,
    json_body: dict[str, Any] | list[Any] | None = None,
) -> ApiProbeResult:
    return probe_request(
        ApiProbeRequest(
            url=url,
            method=method,
            params=params,
            headers=headers,
            cookies=cookies,
            body=body,
            json_body=json_body,
        ),
        timeout=timeout,
    )


def probe_request(request: ApiProbeRequest, timeout: int = 8) -> ApiProbeResult:
    client = CrawlerClient(timeout=timeout, retries=0)
    result = client._request(  # noqa: SLF001 - internal probing needs raw Response metadata.
        request.method.upper(),
        request.url,
        params=request.params,
        headers=request.headers,
        cookies=request.cookies,
        data=request.body,
        json_body=request.json_body,
        source=request.source,
        host_key=request.url,
    )
    return _to_probe_result(request.url, result)


def probe_many(
    urls: list[str],
    workers: int = DEFAULT_WORKERS,
    timeout: int = 8,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    body: str | bytes | dict[str, Any] | None = None,
    json_body: dict[str, Any] | list[Any] | None = None,
) -> list[ApiProbeResult]:
    if not urls:
        return []
    max_workers = max(1, min(workers, len(urls)))
    results: dict[int, ApiProbeResult] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                probe_url,
                url,
                timeout,
                method,
                params,
                headers,
                cookies,
                body,
                json_body,
            ): index
            for index, url in enumerate(urls)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                results[index] = future.result()
            except Exception as exc:
                results[index] = ApiProbeResult(url=urls[index], ok=False, error=PARSE_ERROR, error_detail=str(exc))
    return [results[index] for index in sorted(results)]


def discover_endpoints(page_url: str, timeout: int = 8, limit: int = 100) -> ApiProbeResult:
    client = CrawlerClient(timeout=timeout, retries=0)
    result = client.get_text(page_url, source=SOURCE, host_key=page_url)
    probe = _to_probe_result(page_url, result)
    if limit and probe.discovered_urls:
        probe.discovered_urls = probe.discovered_urls[:limit]
    return probe


def discover_and_probe(
    page_urls: list[str],
    workers: int = DEFAULT_WORKERS,
    timeout: int = 8,
    limit_per_page: int = 100,
    include_pages: bool = False,
) -> list[ApiProbeResult]:
    pages = [discover_endpoints(url, timeout=timeout, limit=limit_per_page) for url in page_urls]
    discovered = []
    seen = set()
    for page in pages:
        for url in page.discovered_urls:
            if url not in seen and not _looks_static_asset(url):
                seen.add(url)
                discovered.append(url)
    probed = probe_many(discovered, workers=workers, timeout=timeout)
    return pages + probed if include_pages else probed


def discover_js_api_candidates(
    page_url: str,
    timeout: int = 8,
    stock_code: str | None = None,
    market: str | None = None,
    limit: int = 300,
) -> list[str]:
    client = CrawlerClient(timeout=timeout, retries=0)
    page = client.get_text(page_url, source=SOURCE, host_key=page_url)
    if not page.ok:
        return []
    html = str(page.data or "")
    script_urls = _extract_script_urls(html, page_url)
    js_texts = []
    for script_url in script_urls[:40]:
        script = client.get_text(script_url, source=SOURCE, host_key=script_url)
        if script.ok:
            js_texts.append(str(script.data or ""))
    secid = _infer_secid(page_url, html, stock_code=stock_code, market=market)
    candidates = _extract_api_candidates("\n".join(js_texts), page_url, secid=secid, limit=limit)
    candidates.extend(_eastmoney_stock_candidates(secid) if secid else [])
    return _dedupe_urls(candidates)[:limit]


def discover_js_apis_and_probe(
    page_url: str,
    workers: int = DEFAULT_WORKERS,
    timeout: int = 8,
    stock_code: str | None = None,
    market: str | None = None,
    limit: int = 300,
) -> list[ApiProbeResult]:
    candidates = discover_js_api_candidates(
        page_url,
        timeout=timeout,
        stock_code=stock_code,
        market=market,
        limit=limit,
    )
    return probe_many(candidates, workers=workers, timeout=timeout)


def load_har_urls(path: str, include_static: bool = False) -> list[str]:
    data = json.loads(open(path, encoding="utf-8-sig").read())
    entries = data.get("log", {}).get("entries", [])
    urls = []
    seen = set()
    for entry in entries:
        request = entry.get("request", {})
        url = request.get("url", "")
        if not url or url in seen:
            continue
        if not include_static and _looks_static_asset(url):
            continue
        seen.add(url)
        urls.append(url)
    return urls


def analyze_har_responses(path: str, include_static: bool = False) -> list[ApiProbeResult]:
    data = json.loads(open(path, encoding="utf-8-sig").read())
    entries = data.get("log", {}).get("entries", [])
    results = []
    for entry in entries:
        request = entry.get("request", {})
        response = entry.get("response", {})
        url = request.get("url", "")
        if not url or (not include_static and _looks_static_asset(url)):
            continue
        content = response.get("content", {})
        text = content.get("text", "") or ""
        mime_type = content.get("mimeType", "") or ""
        status = response.get("status")
        elapsed_ms = int(entry.get("time") or 0)
        fake = CrawlResult(
            ok=bool(status and 200 <= int(status) < 400),
            data=_HarResponse(text=text, status_code=int(status or 0), content_type=mime_type),
            source=SOURCE,
            elapsed_ms=elapsed_ms,
            error="" if status and 200 <= int(status) < 400 else f"HTTP {status}",
        )
        results.append(_to_probe_result(url, fake))
    return results


def export_results(results: list[ApiProbeResult], output_path: str, output_format: str | None = None) -> None:
    path = output_path.lower()
    fmt = (output_format or path.rsplit(".", 1)[-1]).lower()
    rows = [probe_to_dict(result) for result in results]
    if fmt == "json":
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(rows, file, ensure_ascii=False, indent=2)
        return
    if fmt == "jsonl":
        with open(output_path, "w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False) + "\n")
        return
    if fmt == "csv":
        import csv

        fieldnames = [
            "url",
            "ok",
            "status_code",
            "response_kind",
            "elapsed_ms",
            "score",
            "api_hint",
            "stock_fields",
            "json_keys",
            "item_count",
            "error",
            "error_detail",
            "challenge_detected",
            "challenge_type",
        ]
        with open(output_path, "w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        **{key: row.get(key, "") for key in fieldnames},
                        "stock_fields": ",".join(row.get("stock_fields", [])),
                        "json_keys": ",".join(row.get("json_keys", [])),
                    }
                )
        return
    raise ValueError(f"不支持的导出格式: {fmt}")


def save_snapshots(results: list[ApiProbeResult], snapshot_dir: str, only_challenges: bool = False) -> list[str]:
    target = Path(snapshot_dir)
    target.mkdir(parents=True, exist_ok=True)
    saved = []
    for result in results:
        if only_challenges and not result.challenge_detected:
            continue
        digest = hashlib.sha1(result.url.encode("utf-8")).hexdigest()[:12]
        path = target / f"{digest}.json"
        with open(path, "w", encoding="utf-8") as file:
            json.dump(probe_to_dict(result), file, ensure_ascii=False, indent=2)
        saved.append(str(path))
    return saved


def probe_to_dict(result: ApiProbeResult) -> dict[str, Any]:
    return asdict(result)


class _HarResponse:
    def __init__(self, text: str, status_code: int, content_type: str):
        self.text = text
        self.status_code = status_code
        self.headers = {"content-type": content_type}


def _to_probe_result(url: str, result: CrawlResult) -> ApiProbeResult:
    if not result.ok:
        return ApiProbeResult(
            url=url,
            ok=False,
            elapsed_ms=result.elapsed_ms,
            error=result.error,
            error_detail=result.error_detail,
            preview=result.user_message,
        )

    response = result.data
    if isinstance(response, Response) or isinstance(response, _HarResponse):
        text = response.text
        content_type = response.headers.get("content-type", "")
        status_code = response.status_code
    else:
        text = str(response)
        content_type = ""
        status_code = None

    parsed = _parse_payload(text, content_type)
    discovered_urls = _extract_urls(text, url) if parsed["response_kind"] == "html" else []
    stock_fields = _detect_stock_fields(parsed.get("field_names", []), text)
    challenge_type, challenge_reasons = _detect_challenge(text, status_code, content_type)
    semantic = _analyze_api_semantics(url, parsed.get("field_names", []), text)
    score, score_reasons = _score_probe(
        url=url,
        status_code=status_code,
        response_kind=parsed["response_kind"],
        elapsed_ms=result.elapsed_ms,
        item_count=parsed["item_count"],
        stock_fields=stock_fields,
        discovered_urls=discovered_urls,
    )
    return ApiProbeResult(
        url=url,
        ok=True,
        status_code=status_code,
        content_type=content_type,
        response_kind=parsed["response_kind"],
        elapsed_ms=result.elapsed_ms,
        preview=_preview(text),
        json_keys=parsed["json_keys"],
        item_count=parsed["item_count"],
        discovered_urls=discovered_urls,
        score=score,
        score_reasons=score_reasons,
        stock_fields=stock_fields,
        sample_paths=parsed.get("sample_paths", []),
        api_hint=_looks_like_api_url(url, content_type, parsed["response_kind"]),
        challenge_detected=bool(challenge_type),
        challenge_type=challenge_type,
        challenge_reasons=challenge_reasons,
        api_vendor=semantic["vendor"],
        api_category=semantic["category"],
        api_module=semantic["module"],
        semantic_fields=semantic["fields"],
        semantic_summary=semantic["summary"],
    )


def _parse_payload(text: str, content_type: str) -> dict[str, Any]:
    stripped = text.strip()
    if "json" in content_type.lower() or stripped.startswith(("{", "[")):
        try:
            data = json.loads(stripped)
        except Exception:
            return {"response_kind": "text", "json_keys": [], "item_count": None, "field_names": [], "sample_paths": []}
        return _summarize_json(data)
    if "<html" in stripped[:500].lower() or "<!doctype html" in stripped[:500].lower():
        return {"response_kind": "html", "json_keys": [], "item_count": None, "field_names": [], "sample_paths": []}
    return {"response_kind": "text", "json_keys": [], "item_count": None, "field_names": [], "sample_paths": []}


def _summarize_json(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        item_count = len(data)
        field_names, sample_paths = _collect_json_fields(data)
        return {
            "response_kind": "json",
            "json_keys": sorted(str(key) for key in data.keys())[:30],
            "item_count": item_count,
            "field_names": field_names,
            "sample_paths": sample_paths,
        }
    if isinstance(data, list):
        keys = []
        if data and isinstance(data[0], dict):
            keys = sorted(str(key) for key in data[0].keys())[:30]
        field_names, sample_paths = _collect_json_fields(data)
        return {
            "response_kind": "json",
            "json_keys": keys,
            "item_count": len(data),
            "field_names": field_names,
            "sample_paths": sample_paths,
        }
    return {"response_kind": "json", "json_keys": [], "item_count": None, "field_names": [], "sample_paths": []}


def _extract_urls(text: str, base_url: str, limit: int = 100) -> list[str]:
    urls = []
    seen = set()
    for match in ENDPOINT_PATTERN.finditer(text):
        raw_url = match.group("url")
        if raw_url.startswith(("data:", "javascript:", "#")):
            continue
        absolute = urljoin(base_url, raw_url)
        if absolute in seen:
            continue
        seen.add(absolute)
        urls.append(absolute)
        if len(urls) >= limit:
            break
    return urls


def _extract_script_urls(text: str, base_url: str) -> list[str]:
    urls = []
    for src in SCRIPT_SRC_PATTERN.findall(text):
        url = "https:" + src if src.startswith("//") else urljoin(base_url, src)
        urls.append(url)
    return _dedupe_urls(urls)


def _extract_api_candidates(js_text: str, base_url: str, secid: str | None = None, limit: int = 300) -> list[str]:
    candidates = []
    for match in API_PATH_PATTERN.finditer(js_text):
        raw = match.group("path")
        if "concat" in raw or "{{" in raw:
            continue
        url = "https:" + raw if raw.startswith("//") else urljoin(base_url, raw)
        url = url.replace("cb=?", "").replace("?&", "?")
        if secid and "secid=" not in url and any(path in url for path in ("/stock/get", "/stock/details/get", "/stock/trends2/get", "/stock/kline/get", "/slist/get")):
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}secid={secid}"
        if "ut=" not in url and "eastmoney.com" in url:
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}ut=fa5fd1943c7b386f172d6893dbfba10b"
        candidates.append(url)
        if len(candidates) >= limit:
            break
    return _dedupe_urls(candidates)


def _infer_secid(page_url: str, html: str, stock_code: str | None = None, market: str | None = None) -> str | None:
    code = stock_code
    if not code:
        match = re.search(r"(?:sh|sz|bj)(\d{6})", page_url, re.I) or re.search(r"\b([03689]\d{5})\b", html)
        code = match.group(1) if match else None
    if not code:
        return None
    if market is None:
        lowered = page_url.lower()
        if "sh" + code in lowered or code.startswith(("6", "9")):
            market = "1"
        elif "bj" + code in lowered or code.startswith(("8", "4")):
            market = "0"
        else:
            market = "0"
    return f"{market}.{code}"


def _eastmoney_stock_candidates(secid: str) -> list[str]:
    ut = "fa5fd1943c7b386f172d6893dbfba10b"
    fields_stock = "f19,f39,f43,f44,f45,f46,f47,f48,f49,f50,f57,f58,f59,f60,f84,f85,f86,f116,f117,f152,f168,f169,f170,f171"
    return [
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&ut={ut}&fltt=2&invt=2&fields={fields_stock}",
        f"https://push2.eastmoney.com/api/qt/stock/trends2/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut={ut}&iscr=0&iscca=0&ndays=1",
        f"https://push2.eastmoney.com/api/qt/stock/details/get?secid={secid}&ut={ut}&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&pos=-0&iscca=1&wbp2u=",
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&ut={ut}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20250101&end=20500101&lmt=20",
        f"https://push2.eastmoney.com/api/qt/slist/get?fltt=1&invt=2&fields=f12,f13,f14,f3,f128&secid={secid}&ut={ut}&pi=0&po=1&np=1&pz=20&spt=3",
        f"https://push2.eastmoney.com/api/qt/slist/get?fltt=1&invt=2&fields=f12,f13,f14,f3,f127,f128,f136&secid={secid}&ut={ut}&pi=0&po=1&np=1&pz=5&spt=1",
    ]


def _dedupe_urls(urls: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _preview(text: str) -> str:
    return text.strip().replace("\r", "")[:MAX_PREVIEW_CHARS]


def _collect_json_fields(data: Any) -> tuple[list[str], list[str]]:
    counter: Counter[str] = Counter()
    sample_paths: list[str] = []

    def walk(value: Any, path: str = "$") -> None:
        if len(sample_paths) >= MAX_SAMPLE_PATHS:
            return
        if isinstance(value, dict):
            for key, item in value.items():
                key_text = str(key)
                counter[key_text.lower()] += 1
                child_path = f"{path}.{key_text}"
                sample_paths.append(child_path)
                walk(item, child_path)
                if len(sample_paths) >= MAX_SAMPLE_PATHS:
                    return
        elif isinstance(value, list):
            sample_paths.append(f"{path}[]")
            for item in value[:3]:
                walk(item, f"{path}[]")
                if len(sample_paths) >= MAX_SAMPLE_PATHS:
                    return

    walk(data)
    return [name for name, _ in counter.most_common(80)], sample_paths[:MAX_SAMPLE_PATHS]


def _detect_stock_fields(field_names: list[str], text: str) -> list[str]:
    normalized = {name.lower().replace("_", "").replace("-", "") for name in field_names}
    detected = []
    for field_name, aliases in FIELD_ALIASES.items():
        normalized_aliases = {alias.lower().replace("_", "").replace("-", "") for alias in aliases}
        if normalized & normalized_aliases:
            detected.append(field_name)
    if STOCK_CODE_PATTERN.search(text):
        detected.append("stock_code_pattern")
    return sorted(set(detected))


def _detect_challenge(text: str, status_code: int | None, content_type: str) -> tuple[str, list[str]]:
    lowered = text.lower()
    reasons = []
    if status_code in {401, 403, 429}:
        reasons.append(f"HTTP {status_code}")
    for challenge_type, keywords in CHALLENGE_KEYWORDS.items():
        matched = [keyword for keyword in keywords if keyword.lower() in lowered]
        if matched:
            reasons.extend(matched[:5])
            return challenge_type, reasons
    if reasons:
        if status_code == 401:
            return "login_required", reasons
        return "risk_control", reasons
    if "text/html" in content_type.lower() and ("验证" in text or "verify" in lowered):
        return "captcha", ["HTML 验证页面特征"]
    return "", []


def _analyze_api_semantics(url: str, field_names: list[str], text: str) -> dict[str, Any]:
    if "eastmoney.com" not in url.lower():
        return {"vendor": "", "category": "", "module": "", "fields": {}, "summary": ""}

    parsed = urlparse(url)
    path = parsed.path.lower()
    query = dict(parse_qsl(parsed.query))
    fields = _semantic_fields_from_url(query)
    fields.update(_semantic_fields_from_json(field_names))

    category = "unknown"
    module = "东方财富接口"
    if "/api/qt/stock/trends" in path:
        category = "quote_minute"
        module = "分时走势"
        fields.update({"data.trends": "分时K线：时间,开/收/高/低,成交量,成交额,均价"})
    elif "/api/qt/stock/details" in path:
        category = "quote_ticks"
        module = "逐笔成交"
        fields.update({"data.details": "逐笔成交：时间,价格,成交量,方向/性质"})
    elif "/api/qt/stock/kline" in path:
        category = "quote_kline"
        module = "K线行情"
        fields.update({"data.klines": "K线：日期,开盘,收盘,最高,最低,成交量,成交额等"})
    elif "/api/qt/stock/get" in path or "/api/qt/ulist" in path:
        category = "quote_snapshot"
        module = "实时行情/公司核心数据"
    elif "/api/qt/slist" in path:
        spt = query.get("spt", "")
        if spt == "3":
            category = "related_boards"
            module = "所属板块"
        elif spt == "1":
            category = "period_change"
            module = "阶段涨幅"
        else:
            category = "related_list"
            module = "关联行情列表"
    elif "/pc_hsf10/newfinanceanalysis" in path:
        category = "f10_finance"
        module = "F10财务核心指标"
        fields.update({key: value for key, value in F10_FIELD_MAP.items() if key.lower() in set(field_names)})
    elif "/pc_hsf10/companysurvey" in path:
        category = "f10_company"
        module = "F10公司概况"
        fields.update(
            {
                "jbzl.gsmc": "公司名称",
                "jbzl.sshy": "所属行业",
                "jbzl.ssjys": "交易所",
                "jbzl.zczb": "注册资本",
                "jbzl.gsjj": "公司简介",
                "fxxg.ssrq": "上市日期",
            }
        )

    summary_parts = [module]
    if fields:
        summary_parts.append("字段: " + "、".join(list(fields.values())[:10]))
    return {
        "vendor": "eastmoney",
        "category": category,
        "module": module,
        "fields": fields,
        "summary": "；".join(summary_parts),
    }


def _semantic_fields_from_url(query: dict[str, str]) -> dict[str, str]:
    result = {}
    for key in ("fields", "fields1", "fields2", "columns"):
        raw = query.get(key, "")
        for f in raw.split(","):
            f = f.strip()
            if not f:
                continue
            if f in EASTMONEY_FIELD_MAP:
                result[f] = EASTMONEY_FIELD_MAP[f]
            elif f in F10_FIELD_MAP:
                result[f] = F10_FIELD_MAP[f]
    return result


def _semantic_fields_from_json(field_names: list[str]) -> dict[str, str]:
    lowered = {name.lower(): name for name in field_names}
    result = {}
    for key, label in F10_FIELD_MAP.items():
        if key.lower() in lowered:
            result[lowered[key.lower()]] = label
    for key, label in EASTMONEY_FIELD_MAP.items():
        if key.lower() in lowered:
            result[lowered[key.lower()]] = label
    return result


def _score_probe(
    url: str,
    status_code: int | None,
    response_kind: str,
    elapsed_ms: int,
    item_count: int | None,
    stock_fields: list[str],
    discovered_urls: list[str],
) -> tuple[int, list[str]]:
    score = 0
    reasons = []
    if status_code and 200 <= status_code < 300:
        score += 25
        reasons.append("HTTP 2xx")
    if response_kind == "json":
        score += 25
        reasons.append("JSON 响应")
    elif response_kind == "html" and discovered_urls:
        score += 10
        reasons.append("HTML 中发现 URL")
    if _looks_like_api_url(url, "", response_kind):
        score += 15
        reasons.append("URL 疑似 API")
    if item_count:
        score += min(15, item_count)
        reasons.append(f"包含 {item_count} 个顶层元素")
    if stock_fields:
        score += min(30, len(stock_fields) * 8)
        reasons.append("命中股票字段: " + ",".join(stock_fields))
    if elapsed_ms and elapsed_ms <= 1200:
        score += 5
        reasons.append("响应较快")
    return min(score, 100), reasons


def _looks_like_api_url(url: str, content_type: str, response_kind: str) -> bool:
    lowered = url.lower()
    parsed = urlparse(url)
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query)}
    api_tokens = ("api", "ajax", "json", "quote", "stock", "fund", "market", "list", "rank", "data", "push2")
    if any(token in lowered for token in api_tokens):
        return True
    if query_keys & {"code", "symbol", "secid", "secucode", "page", "pn", "pz", "fields", "ut"}:
        return True
    return "json" in content_type.lower() or response_kind == "json"


def _looks_static_asset(url: str) -> bool:
    lowered = url.lower().split("?", 1)[0]
    return lowered.endswith(
        (
            ".js",
            ".css",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".svg",
            ".ico",
            ".woff",
            ".woff2",
            ".ttf",
            ".map",
        )
    )
