"""
utils.py - 公共工具函数
供 modules/ 下所有模块使用，禁止在此文件外定义重复的工具函数。
"""

from __future__ import annotations

import os


def normalize_symbol(symbol: str) -> str:
    """去掉 SH/SZ/BJ 后缀，只保留数字部分。"""
    s = symbol.strip().upper()
    for suffix in (".SH", ".SZ", ".BJ", "SH", "SZ", "BJ"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    return s


def to_tencent_code(symbol: str) -> str:
    """转为腾讯财经格式 (sh600519 / sz000001 / bj888888)。"""
    s = normalize_symbol(symbol)
    if s.startswith(("4", "8")):
        return f"bj{s}"
    if s.startswith(("6", "9", "5")):
        return f"sh{s}"
    return f"sz{s}"


def to_sina_code(symbol: str) -> str:
    """转为新浪财经格式 (sh600519 / sz000001)。"""
    s = normalize_symbol(symbol)
    prefix = "sh" if s.startswith(("6", "9", "5")) else "sz"
    return f"{prefix}{s}"


def to_eastmoney_secid(symbol: str) -> str:
    """转为东方财富 secid 格式 (1.600519 / 0.000858)。"""
    s = normalize_symbol(symbol)
    prefix = "1" if s.startswith(("5", "6", "9")) else "0"
    return f"{prefix}.{s}"


def _parse_market(symbol: str) -> str:
    """从股票代码中提取市场前缀 (sh / sz / bj)。"""
    sym = normalize_symbol(symbol)
    if sym.startswith(("4", "8")):
        return "bj"
    if sym.startswith(("6", "9", "5")):
        return "sh"
    return "sz"


def to_full_code(symbol: str) -> str:
    """将裸代码转为带市场后缀的全码 (600519.SH / 000858.SZ / 888888.BJ)。"""
    s = normalize_symbol(symbol)
    if s.startswith(("4", "8")):
        return f"{s}.BJ"
    if s.startswith(("6", "9", "5")):
        return f"{s}.SH"
    return f"{s}.SZ"


def _strip_suffix(symbol: str) -> str:
    """去掉 .SH / .SZ / .BJ 后缀，保留数字部分（同 normalize_symbol 但不过滤纯字母后缀）。"""
    return symbol.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")


def _clear_all_proxy() -> None:
    """
    清除所有代理环境变量（模块加载时一次性执行）。
    同时设置 NO_PROXY=* 防止 urllib3 使用系统代理。
    """
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                 "ALL_PROXY", "all_proxy",
                 "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY"]:
        os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"
