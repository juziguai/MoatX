"""Crawler data source diagnostics."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from typing import Any
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.crawler import sector  # noqa: E402
from modules.crawler.models import CrawlResult, SOURCE_UNAVAILABLE  # noqa: E402
from modules.akshare_compat import import_akshare  # noqa: E402
from modules.crawler.fundflow import get_individual_fund_flow  # noqa: E402
from modules.crawler.ths_fund_flow import get_hexin_v_header  # noqa: E402
from modules.sector_tags import SectorTagProvider  # noqa: E402


def run_diagnose(source: str = "all", as_json: bool = False, fresh: bool = False) -> str:
    checks = []
    selected = source.lower()
    use_cache = not fresh

    if selected in ("all", "sector", "eastmoney"):
        checks.append(("akshare", _run_check(_check_akshare, use_cache=use_cache, fresh=fresh)))
        checks.append(("THS header", _run_check(_check_ths_header, use_cache=use_cache, fresh=fresh)))
        checks.append(("行业板块", _run_check(sector.get_industry_boards, use_cache=use_cache, fresh=fresh)))
        checks.append(("概念板块", _run_check(sector.get_concept_boards, use_cache=use_cache, fresh=fresh)))
        checks.append(("动态行业成分", _run_check(_check_industry_members, use_cache=use_cache, fresh=fresh)))
        checks.append(("动态概念成分", _run_check(_check_concept_members, use_cache=use_cache, fresh=fresh)))

    if selected in ("all", "fundflow", "eastmoney"):
        checks.append(("个股资金流", _run_check(_check_fundflow, use_cache=use_cache, fresh=fresh)))

    if not checks:
        checks.append(("行业板块", _run_check(sector.get_industry_boards, use_cache=use_cache, fresh=fresh)))
        checks.append(("概念板块", _run_check(sector.get_concept_boards, use_cache=use_cache, fresh=fresh)))

    payload = [_format_check(name, result) for name, result in checks]
    if as_json:
        return json.dumps(payload, ensure_ascii=False, indent=2)
    return "\n".join(_format_text(item) for item in payload)


def _run_check(fetcher, use_cache: bool, fresh: bool) -> CrawlResult:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        result = fetcher(use_cache=use_cache)
    return _fresh_check(result, fresh)


def _check_akshare(use_cache: bool = True) -> CrawlResult:
    ak = import_akshare()
    if not ak:
        return CrawlResult(
            ok=False,
            source="akshare",
            error=SOURCE_UNAVAILABLE,
            error_detail=str(getattr(ak, "error", "")),
            user_message="akshare unavailable",
        )
    return CrawlResult(ok=True, data={"version": getattr(ak, "__version__", "")}, source="akshare")


def _check_ths_header(use_cache: bool = True) -> CrawlResult:
    value = get_hexin_v_header()
    if not value:
        return CrawlResult(ok=False, source="ths_header", error=SOURCE_UNAVAILABLE)
    return CrawlResult(ok=True, data={"length": len(value)}, source="ths_header")


def _check_fundflow(use_cache: bool = True) -> CrawlResult:
    return get_individual_fund_flow("002342", use_cache=use_cache, days=1)


def _check_industry_members(use_cache: bool = True) -> CrawlResult:
    return _check_members("元件", "industry")


def _check_concept_members(use_cache: bool = True) -> CrawlResult:
    return _check_members("PCB概念", "concept")


def _check_members(target: str, target_type: str) -> CrawlResult:
    members = SectorTagProvider().get_members(target, target_type)
    if members.empty:
        return CrawlResult(
            ok=False,
            source="sector_members",
            error=SOURCE_UNAVAILABLE,
            user_message=f"{target} dynamic members unavailable",
        )
    source = str(members["source"].iloc[0]) if "source" in members.columns else "sector_members"
    return CrawlResult(ok=True, data=members, source=source)


def _fresh_check(result: CrawlResult, fresh: bool) -> CrawlResult:
    if not fresh or not result.from_cache:
        return result

    result.ok = False
    result.error = result.error or SOURCE_UNAVAILABLE
    result.user_message = "实时数据源不可用，fresh 模式不接受缓存结果"
    result.warnings.append("fresh 模式检测到缓存回退，已标记为实时不可用")
    return result


def _format_check(name: str, result: CrawlResult) -> dict[str, Any]:
    rows = 0
    if result.data is not None and hasattr(result.data, "__len__"):
        rows = len(result.data)
    return {
        "name": name,
        "ok": result.ok,
        "rows": rows,
        "source": result.source,
        "from_cache": result.from_cache,
        "cached_at": result.cached_at,
        "trade_date": result.trade_date,
        "error": result.error,
        "error_detail": result.error_detail,
        "user_message": result.user_message,
        "warnings": result.warnings,
    }


def _format_text(item: dict[str, Any]) -> str:
    status = "✅" if item["ok"] else "❌"
    freshness = _freshness_label(item)
    warning = f" ⚠️ {'; '.join(item['warnings'])}" if item["warnings"] else ""
    message = item["user_message"] or item["error_detail"] or item["error"]
    if item["ok"]:
        return f"{item['name']}: {status} {item['rows']} 条 | 来源：{freshness}{warning}"
    return f"{item['name']}: {status} {item['error']} | 来源：{freshness} | {message}{warning}"


def _freshness_label(item: dict[str, Any]) -> str:
    source = item.get("source") or "未知"
    cached_at = item.get("cached_at") or ""
    trade_date = item.get("trade_date") or ""
    if item.get("from_cache"):
        if cached_at:
            return f"{source} 缓存 {cached_at}"
        if trade_date:
            return f"{source} 缓存快照 {trade_date}"
        return f"{source} 缓存"
    return f"{source} 实时"


def main() -> None:
    parser = argparse.ArgumentParser(description="MoatX 爬虫数据源诊断")
    parser.add_argument("--source", default="all", help="数据源: all/sector/eastmoney/sina")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")
    parser.add_argument("--fresh", action="store_true", help="跳过缓存，测试实时数据源")
    args = parser.parse_args()
    print(run_diagnose(source=args.source, as_json=args.json, fresh=args.fresh))


if __name__ == "__main__":
    main()
