"""CLI helper for generic website/API probing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.crawler import api_probe


def run_probe(
    urls: list[str],
    har_files: list[str] | None = None,
    workers: int = api_probe.DEFAULT_WORKERS,
    timeout: int = 8,
    as_json: bool = False,
    discover: bool = False,
    probe_discovered: bool = False,
    probe_js_apis: bool = False,
    include_static: bool = False,
    analyze_har_body: bool = False,
    stock_code: str | None = None,
    market: str | None = None,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    params: dict | None = None,
    body: str | None = None,
    json_body: dict | list | None = None,
    min_score: int | None = None,
    sort_by_score: bool = False,
    output: str | None = None,
    output_format: str | None = None,
    snapshot_dir: str | None = None,
    snapshot_challenges_only: bool = False,
    semantic_only: bool = False,
) -> str:
    urls = _merge_urls(urls, har_files or [], include_static=include_static)
    if analyze_har_body:
        results = []
        for har_file in har_files or []:
            results.extend(api_probe.analyze_har_responses(har_file, include_static=include_static))
    elif probe_js_apis:
        results = []
        for url in urls:
            results.extend(
                api_probe.discover_js_apis_and_probe(
                    url,
                    workers=workers,
                    timeout=timeout,
                    stock_code=stock_code,
                    market=market,
                )
            )
    elif probe_discovered:
        results = api_probe.discover_and_probe(urls, workers=workers, timeout=timeout, include_pages=True)
    elif discover:
        results = [api_probe.discover_endpoints(url, timeout=timeout) for url in urls]
    else:
        results = api_probe.probe_many(
            urls,
            workers=workers,
            timeout=timeout,
            method=method,
            params=params,
            headers=headers,
            cookies=cookies,
            body=body,
            json_body=json_body,
        )

    if min_score is not None:
        results = [result for result in results if result.score >= min_score]
    if sort_by_score:
        results = sorted(results, key=lambda result: result.score, reverse=True)
    if output:
        api_probe.export_results(results, output, output_format=output_format)
    if snapshot_dir:
        api_probe.save_snapshots(results, snapshot_dir, only_challenges=snapshot_challenges_only)

    payload = [api_probe.probe_to_dict(result) for result in results]
    if as_json:
        return json.dumps(payload, ensure_ascii=False, indent=2)
    if semantic_only:
        return "\n".join(_format_semantic(item) for item in payload)
    return "\n".join(_format_text(item) for item in payload)


def _format_text(item: dict) -> str:
    status = "✅" if item["ok"] else "❌"
    if not item["ok"]:
        return f"{status} {item['url']} | {item['error']} | {item['error_detail']}"
    detail = item["response_kind"] or item["content_type"] or "unknown"
    extras = []
    if item["json_keys"]:
        extras.append("keys=" + ",".join(item["json_keys"][:8]))
    if item["item_count"] is not None:
        extras.append(f"items={item['item_count']}")
    if item["discovered_urls"]:
        extras.append(f"discovered={len(item['discovered_urls'])}")
    if item["stock_fields"]:
        extras.append("stock=" + ",".join(item["stock_fields"][:8]))
    if item["challenge_detected"]:
        extras.append(f"challenge={item['challenge_type']}")
    if item.get("api_module"):
        extras.append(f"module={item['api_module']}")
    extras.append(f"score={item['score']}")
    suffix = " | " + " | ".join(extras) if extras else ""
    return f"{status} {item['url']} | {item['status_code']} | {detail} | {item['elapsed_ms']}ms{suffix}"


def _format_semantic(item: dict) -> str:
    status = "✅" if item["ok"] else "❌"
    module = item.get("api_module") or "未知模块"
    category = item.get("api_category") or "unknown"
    fields = item.get("semantic_fields") or {}
    field_text = "、".join(dict.fromkeys(fields.values()).keys()) if fields else "-"
    return f"{status} {module} [{category}] score={item.get('score', 0)}\n  {item['url']}\n  字段: {field_text}"


def main() -> None:
    parser = argparse.ArgumentParser(description="MoatX 通用网站/API 探测")
    parser.add_argument("urls", nargs="*", help="待探测 URL，支持多个")
    parser.add_argument("--file", help="从文本文件读取 URL，每行一个")
    parser.add_argument("--har", action="append", default=[], help="导入浏览器 HAR 文件，可重复")
    parser.add_argument("--include-static", action="store_true", help="HAR 导入时保留 JS/CSS/图片等静态资源")
    parser.add_argument("--analyze-har-body", action="store_true", help="离线分析 HAR 中已保存的 response body，不重新请求")
    parser.add_argument("--workers", type=int, default=api_probe.DEFAULT_WORKERS, help="并发数")
    parser.add_argument("--timeout", type=int, default=8, help="单请求超时秒数")
    parser.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    parser.add_argument("--discover", action="store_true", help="从网页中提取疑似 API/资源 URL")
    parser.add_argument("--probe-discovered", action="store_true", help="先发现网页 URL，再批量探测发现到的非静态接口")
    parser.add_argument("--probe-js-apis", action="store_true", help="抓取页面外部 JS，提取 API 候选并批量探测")
    parser.add_argument("--stock-code", help="股票代码，用于补全页面 API 模板")
    parser.add_argument("--market", help="市场代码，东方财富 secid 前缀，如 1=沪市,0=深/北")
    parser.add_argument("--method", default="GET", help="HTTP 方法，如 GET/POST")
    parser.add_argument("--header", action="append", default=[], help="请求头，格式 Key: Value，可重复")
    parser.add_argument("--headers-json", help="JSON 对象格式请求头")
    parser.add_argument("--cookie", action="append", default=[], help="Cookie，格式 name=value，可重复")
    parser.add_argument("--cookies-json", help="JSON 对象格式 Cookie")
    parser.add_argument("--cookie-file", help="Cookie 文件，支持 JSON 对象或 Netscape cookies.txt")
    parser.add_argument("--param", action="append", default=[], help="Query 参数，格式 key=value，可重复")
    parser.add_argument("--params-json", help="JSON 对象格式 query params")
    parser.add_argument("--body", help="原始请求体")
    parser.add_argument("--body-file", help="从文件读取原始请求体")
    parser.add_argument("--json-body", help="JSON 请求体")
    parser.add_argument("--json-body-file", help="从文件读取 JSON 请求体")
    parser.add_argument("--min-score", type=int, help="只输出评分不低于该值的接口")
    parser.add_argument("--sort-score", action="store_true", help="按接口评分降序输出")
    parser.add_argument("--output", help="导出结果文件，支持 .json/.jsonl/.csv")
    parser.add_argument("--output-format", choices=["json", "jsonl", "csv"], help="指定导出格式")
    parser.add_argument("--snapshot-dir", help="保存响应分析快照目录")
    parser.add_argument("--snapshot-challenges-only", action="store_true", help="仅保存验证码/风控/登录校验类快照")
    parser.add_argument("--semantic-only", action="store_true", help="仅输出接口语义摘要")
    args = parser.parse_args()
    urls = _load_urls(args.urls, args.file)
    if not urls:
        if not args.har:
            parser.error("至少提供一个 URL，或使用 --file/--har 指定输入")
    try:
        headers = _parse_mapping(args.header, args.headers_json, separator=":")
        cookies = _merge_cookie_inputs(args.cookie, args.cookies_json, args.cookie_file)
        params = _parse_mapping(args.param, args.params_json, separator="=") or None
        json_text = Path(args.json_body_file).read_text(encoding="utf-8-sig") if args.json_body_file else args.json_body
        json_body = json.loads(json_text) if json_text else None
        body = Path(args.body_file).read_text(encoding="utf-8-sig") if args.body_file else args.body
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    print(
        run_probe(
            urls=urls,
            har_files=args.har,
            workers=args.workers,
            timeout=args.timeout,
            as_json=args.as_json,
            discover=args.discover,
            probe_discovered=args.probe_discovered,
            probe_js_apis=args.probe_js_apis,
            include_static=args.include_static,
            analyze_har_body=args.analyze_har_body,
            stock_code=args.stock_code,
            market=args.market,
            method=args.method,
            headers=headers,
            cookies=cookies,
            params=params,
            body=body,
            json_body=json_body,
            min_score=args.min_score,
            sort_by_score=args.sort_score,
            output=args.output,
            output_format=args.output_format,
            snapshot_dir=args.snapshot_dir,
            snapshot_challenges_only=args.snapshot_challenges_only,
            semantic_only=args.semantic_only,
        )
    )


def _load_urls(urls: list[str], file_path: str | None) -> list[str]:
    loaded = list(urls)
    if file_path:
        loaded.extend(
            line.strip()
            for line in Path(file_path).read_text(encoding="utf-8-sig").splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
    return loaded


def _merge_urls(urls: list[str], har_files: list[str], include_static: bool = False) -> list[str]:
    merged = []
    seen = set()
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            merged.append(url)
    for har_file in har_files:
        for url in api_probe.load_har_urls(har_file, include_static=include_static):
            if url not in seen:
                seen.add(url)
                merged.append(url)
    return merged


def _parse_mapping(items: list[str], json_text: str | None, separator: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    if json_text:
        parsed.update(_parse_json_object(json_text, "--headers-json/--cookies-json"))
    for item in items:
        if separator not in item:
            raise ValueError(f"参数格式错误: {item}")
        key, value = item.split(separator, 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _merge_cookie_inputs(items: list[str], json_text: str | None, file_path: str | None) -> dict[str, str]:
    cookies = _parse_mapping(items, json_text, separator="=")
    if file_path:
        cookies.update(_load_cookie_file(file_path))
    return cookies


def _load_cookie_file(file_path: str) -> dict[str, str]:
    text = Path(file_path).read_text(encoding="utf-8-sig")
    stripped = text.strip()
    if not stripped:
        return {}
    if stripped.startswith("{"):
        return {str(key): str(value) for key, value in _parse_json_object(stripped, "--cookie-file").items()}
    cookies = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "\t" in line:
            parts = line.split("\t")
            if len(parts) >= 7:
                cookies[parts[5]] = parts[6]
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


def _parse_json_object(text: str, option_name: str) -> dict:
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"{option_name} 必须是 JSON 对象")
    return data


if __name__ == "__main__":
    main()
