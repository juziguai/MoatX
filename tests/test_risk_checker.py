"""P3: risk_checker.py 财务风险检测测试

注意：财务风险检测依赖 akshare 和网络请求，
这里只测试不依赖网络的逻辑（_risk_level 分级）。
真实网络测试标记为 @pytest.mark.integration。
"""

import pytest
from modules.risk_checker import FinancialRiskChecker


class TestRiskLevel:
    def test_risk_level_extreme(self):
        checker = FinancialRiskChecker(None)
        assert checker._risk_level(70) == "极高风险"
        assert checker._risk_level(100) == "极高风险"

    def test_risk_level_high(self):
        checker = FinancialRiskChecker(None)
        assert checker._risk_level(50) == "高风险"
        assert checker._risk_level(69) == "高风险"

    def test_risk_level_medium(self):
        checker = FinancialRiskChecker(None)
        assert checker._risk_level(30) == "中等风险"
        assert checker._risk_level(49) == "中等风险"

    def test_risk_level_low(self):
        checker = FinancialRiskChecker(None)
        assert checker._risk_level(15) == "低风险"
        assert checker._risk_level(29) == "低风险"

    def test_risk_level_basic(self):
        checker = FinancialRiskChecker(None)
        assert checker._risk_level(0) == "基本无风险"
        assert checker._risk_level(14) == "基本无风险"


class TestParseNumber:
    def test_int(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number(42) == 42.0

    def test_float(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number(3.14) == 3.14

    def test_string_with_comma(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number("1,234.56") == 1234.56

    def test_string_with_spaces(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number("  42  ") == 42.0

    def test_invalid_string(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number("abc") == 0.0

    def test_none(self):
        checker = FinancialRiskChecker(None)
        assert checker._parse_number(None) == 0.0
