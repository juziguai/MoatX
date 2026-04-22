"""
tests/test_portfolio.py - 持仓管理单元测试
"""

import os
import pytest
import pandas as pd
from modules.portfolio import Portfolio
from modules.alerter import Alerter


@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test_portfolio.db")
    pf = Portfolio(db_path=db)
    yield db
    pf.close()
    try:
        if os.path.exists(db):
            os.remove(db)
    except PermissionError:
        pass  # Windows 保留文件锁


class TestPortfolio:

    def test_add_and_list(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        pf.add_holding("600519", "贵州茅台", shares=100, cost_price=1800)
        pf.add_holding("000858", "五粮液", shares=200, cost_price=150)

        df = pf.list_holdings()
        assert len(df) == 2
        symbols = df["symbol"].tolist()
        assert "600519" in symbols
        assert "000858" in symbols

    def test_normalize_symbol(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        pf.add_holding("600519.SH", shares=10)
        pf.add_holding("000858.sz", shares=20)

        df = pf.list_holdings()
        assert "600519" in df["symbol"].tolist()
        assert "000858" in df["symbol"].tolist()

    def test_remove_holding(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        pf.add_holding("600519", shares=100)
        pf.remove_holding("600519")
        assert pf.get_holding("600519") is None

    def test_import_parsed_results(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        results = [
            ("600519", "贵州茅台", 100, 1800),
            ("000858", "五粮液", 200, 150),
        ]
        added = pf.import_parsed_results(results)
        assert len(added) == 2
        df = pf.list_holdings()
        assert len(df) == 2

    def test_duplicate_symbol_replace(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        pf.add_holding("600519", "贵州茅台", shares=100, cost_price=1800)
        pf.add_holding("600519", "贵州茅台更名", shares=150, cost_price=2000)
        df = pf.list_holdings()
        assert len(df) == 1
        row = pf.get_holding("600519")
        assert row["shares"] == 150
        assert row["cost_price"] == 2000

    def test_config(self, tmp_db):
        pf = Portfolio(db_path=tmp_db)
        pf.set_config("feishu_webhook", "https://open.feishu.cn/...")
        assert pf.get_config("feishu_webhook") == "https://open.feishu.cn/..."
        assert pf.get_config("nonexistent", "default") == "default"


class TestAlerter:

    def test_format_alert_report_empty(self):
        report = Alerter.format_alert_report([])
        assert "暂无预警" in report

    def test_format_alert_report_with_alerts(self):
        alerts = [
            {"symbol": "600519", "type": "kdj_overbought", "msg": "KDJ 超买 J=95.0，现价 1800元"},
            {"symbol": "000858", "type": "rsi_oversold", "msg": "RSI 超卖 RSI12=20.0"},
        ]
        report = Alerter.format_alert_report(alerts)
        assert "600519" in report
        assert "000858" in report
        assert "KDJ 超买" in report


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
