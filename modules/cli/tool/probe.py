"""
modules/cli/tool/probe.py - API 探测
"""

import json


def _parse_mapping(items, json_str, separator="="):
    """解析键值对"""
    result = {}
    if items:
        for item in items:
            if separator in item:
                k, v = item.split(separator, 1)
                result[k] = v
    if json_str:
        try:
            result.update(json.loads(json_str))
        except Exception:
            pass
    return result if result else None


def cmd_probe_api(args):
    from scripts.probe_api import run_probe

    urls = _load_urls(args.urls, args.file)
    headers = _parse_mapping(args.header, args.headers_json, separator=":")
    cookies = _parse_cookie_inputs(args.cookie, args.cookies_json, args.cookie_file)
    params = _parse_mapping(args.param, args.params_json, separator="=") or None

    json_text = None
    if args.json_body_file:
        with open(args.json_body_file, encoding="utf-8-sig") as f:
            json_text = f.read()
    elif args.json_body:
        json_text = args.json_body
    json_body = json.loads(json_text) if json_text else None

    body = None
    if args.body_file:
        with open(args.body_file, encoding="utf-8-sig") as f:
            body = f.read()
    elif args.body:
        body = args.body

    print(run_probe(
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
    ))


def _load_urls(urls, file):
    if urls:
        return urls
    if file:
        with open(file, encoding="utf-8-sig") as f:
            return [line.strip() for line in f if line.strip()]
    return []


def _parse_cookie_inputs(cookie_list, cookies_json, cookie_file):
    result = {}
    if cookie_list:
        for c in cookie_list:
            if "=" in c:
                k, v = c.split("=", 1)
                result[k] = v
    if cookies_json:
        try:
            result.update(json.loads(cookies_json))
        except Exception:
            pass
    if cookie_file:
        with open(cookie_file, encoding="utf-8-sig") as f:
            content = f.read().strip()
            if content.startswith("{"):
                result.update(json.loads(content))
            else:
                for line in content.split("\n"):
                    if "=" in line and not line.strip().startswith("#"):
                        k, v = line.strip().split("=", 1)
                        result[k] = v
    return result if result else None
