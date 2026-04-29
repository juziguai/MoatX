"""P0: utils.py 公共工具函数测试"""

import pytest
from modules.utils import (
    normalize_symbol,
    to_tencent_code,
    to_sina_code,
    to_eastmoney_secid,
    to_full_code,
    _parse_market,
    _strip_suffix,
)


class TestNormalizeSymbol:
    def test_bare_code(self):
        assert normalize_symbol("600519") == "600519"

    def test_with_dot_sh_suffix(self):
        assert normalize_symbol("600519.SH") == "600519"

    def test_with_dot_sz_suffix(self):
        assert normalize_symbol("000858.SZ") == "000858"

    def test_with_dot_bj_suffix(self):
        assert normalize_symbol("888888.BJ") == "888888"

    def test_with_trailing_SH(self):
        """无点的后缀 SH"""
        assert normalize_symbol("600519SH") == "600519"

    def test_with_trailing_SZ(self):
        assert normalize_symbol("000858SZ") == "000858"

    def test_with_trailing_BJ(self):
        assert normalize_symbol("430001BJ") == "430001"

    def test_lowercase_dot(self):
        assert normalize_symbol("600519.sh") == "600519"

    def test_with_spaces(self):
        assert normalize_symbol("  600519  ") == "600519"
        assert normalize_symbol("830999.BJ") == "830999"

    def test_688_prefix(self):
        assert normalize_symbol("688981.SH") == "688981"


class TestToTencentCode:
    def test_sh_code(self):
        assert to_tencent_code("600519") == "sh600519"
        assert to_tencent_code("600519.SH") == "sh600519"

    def test_sz_code(self):
        assert to_tencent_code("000858") == "sz000858"
        assert to_tencent_code("000858.SZ") == "sz000858"

    def test_bj_4_code(self):
        assert to_tencent_code("430001") == "bj430001"
        assert to_tencent_code("430001.BJ") == "bj430001"

    def test_bj_8_code(self):
        assert to_tencent_code("830999") == "bj830999"


class TestToSinaCode:
    def test_sh_code(self):
        assert to_sina_code("600519") == "sh600519"
        assert to_sina_code("000858") == "sz000858"

    def test_sz_code(self):
        assert to_sina_code("000858.SZ") == "sz000858"

    def test_bj_code(self):
        assert to_sina_code("430001") == "sz430001"  # Sina 不区分 BJ


class TestToEastmoneySecid:
    def test_sh_code(self):
        assert to_eastmoney_secid("600519") == "1.600519"
        assert to_eastmoney_secid("600519.SH") == "1.600519"

    def test_sz_code(self):
        assert to_eastmoney_secid("000858") == "0.000858"
        assert to_eastmoney_secid("000858.SZ") == "0.000858"

    def test_bj_code(self):
        assert to_eastmoney_secid("430001") == "0.430001"


class TestToFullCode:
    def test_sh_code(self):
        assert to_full_code("600519") == "600519.SH"

    def test_sz_code(self):
        assert to_full_code("000858") == "000858.SZ"

    def test_bj_code(self):
        assert to_full_code("430001") == "430001.BJ"
        assert to_full_code("830999") == "830999.BJ"


class TestParseMarket:
    def test_sh(self):
        assert _parse_market("600519") == "sh"
        assert _parse_market("600519.SH") == "sh"

    def test_sz(self):
        assert _parse_market("000858") == "sz"
        assert _parse_market("000858.SZ") == "sz"

    def test_bj_4(self):
        assert _parse_market("430001") == "bj"

    def test_bj_8(self):
        assert _parse_market("830999") == "bj"


class TestStripSuffix:
    def test_strips_sh(self):
        assert _strip_suffix("600519.SH") == "600519"

    def test_strips_sz(self):
        assert _strip_suffix("000858.SZ") == "000858"

    def test_strips_bj(self):
        assert _strip_suffix("430001.BJ") == "430001"

    def test_passthrough_bare(self):
        assert _strip_suffix("600519") == "600519"
