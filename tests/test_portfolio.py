"""P1: portfolio.py 持仓管理测试"""

import pytest
import sqlite3
from modules.portfolio import Portfolio


@pytest.fixture
def portfolio(mem_db):
    """使用内存数据库的 Portfolio 实例"""
    return Portfolio(db_path=":memory:")


class TestHoldingsCRUD:
    def test_add_holding(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        holding = portfolio.get_holding("600519")
        assert holding is not None
        assert holding["symbol"] == "600519"
        assert holding["shares"] == 100

    def test_add_holding_negative_shares_rejected(self, portfolio):
        with pytest.raises(ValueError, match="shares 必须"):
            portfolio.add_holding("600519", "贵州茅台", -10, 1800.0)

    def test_add_holding_negative_cost_rejected(self, portfolio):
        with pytest.raises(ValueError, match="cost_price 必须"):
            portfolio.add_holding("600519", "贵州茅台", 100, -100.0)

    def test_remove_holding(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        portfolio.remove_holding("600519")
        assert portfolio.get_holding("600519") is None

    def test_list_holdings_empty(self, portfolio):
        df = portfolio.list_holdings()
        assert df.empty

    def test_list_holdings_with_data(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        portfolio.add_holding("000858", "五粮液", 200, 150.0)
        df = portfolio.list_holdings()
        assert len(df) == 2
        symbols = set(df["symbol"].tolist())
        assert "600519" in symbols
        assert "000858" in symbols

    def test_add_holding_normalizes_symbol(self, portfolio):
        portfolio.add_holding("600519.SH", "贵州茅台", 100, 1800.0)
        holding = portfolio.get_holding("600519")
        assert holding is not None


class TestRefreshHoldings:
    def test_refresh_holdings_no_quotes(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        count = portfolio.refresh_holdings({})
        assert count == 0

    def test_refresh_holdings_success(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        quotes = {
            "600519.SH": {"code": "600519", "name": "贵州茅台", "price": 1900.0,
                          "change_pct": 1.5, "volume": 1000000}
        }
        count = portfolio.refresh_holdings(quotes)
        assert count == 1
        holding = portfolio.get_holding("600519")
        assert holding["current_price"] == 1900.0


class TestSnapshots:
    def test_insert_snapshot_insert_only(self, portfolio):
        import time
        portfolio.insert_snapshot("2026-04-26", "600519", "贵州茅台",
                                100, 1850.0, 1800.0, 185000.0,
                                5000.0, 2.78, 50.0)
        # 同一股票同一日期再次插入，不应覆盖（INSERT 而非 REPLACE）
        portfolio.insert_snapshot("2026-04-26", "600519", "贵州茅台",
                                100, 1860.0, 1800.0, 186000.0,
                                6000.0, 3.33, 50.0)
        # 验证有两条记录
        df = portfolio.db.execute(
            "SELECT COUNT(*) FROM snapshots WHERE date='2026-04-26' AND symbol='600519'"
        ).fetchone()[0]
        assert df == 2


class TestDailyPnL:
    def test_insert_daily_pnl_insert_only(self, portfolio):
        portfolio.insert_daily_pnl("2026-04-25", "600519", "贵州茅台", 5000.0, 2.5)
        portfolio.insert_daily_pnl("2026-04-25", "600519", "贵州茅台", 6000.0, 3.0)
        # 验证有两条记录
        count = portfolio.db.execute(
            "SELECT COUNT(*) FROM daily_pnl WHERE date='2026-04-25' AND symbol='600519'"
        ).fetchone()[0]
        assert count == 2


class TestRecordTrade:
    def test_record_trade_buy(self, portfolio):
        portfolio.record_trade("2026-04-26", "BUY", "600519", "贵州茅台",
                               100, 1850.0, 185000.0)
        holding = portfolio.get_holding("600519")
        assert holding is not None
        assert holding["shares"] == 100

    def test_record_trade_sell_removes_holding(self, portfolio):
        portfolio.add_holding("600519", "贵州茅台", 100, 1800.0)
        portfolio.record_trade("2026-04-26", "SELL", "600519", "贵州茅台",
                               100, 1900.0, 190000.0)
        holding = portfolio.get_holding("600519")
        assert holding is None

    def test_record_trade_invalid_shares(self, portfolio):
        with pytest.raises(ValueError, match="交易股数必须"):
            portfolio.record_trade("2026-04-26", "BUY", "600519", "贵州茅台",
                                   0, 1850.0, 0.0)

    def test_record_trade_negative_price(self, portfolio):
        with pytest.raises(ValueError, match="交易价格不能为负"):
            portfolio.record_trade("2026-04-26", "BUY", "600519", "贵州茅台",
                                   100, -100.0, -10000.0)


class TestCandidates:
    def test_add_candidate(self, portfolio):
        result = portfolio.add_candidate("300750", "宁德时代", rec_rank=1,
                                         entry_price=500.0, rec_pct_change=5.0)
        assert result is True

    def test_update_candidate_result_insert_only(self, portfolio):
        portfolio.add_candidate("300750", "宁德时代", rec_rank=1, entry_price=500.0)
        portfolio.update_candidate_result("300750", 520.0, 4.0)
        portfolio.update_candidate_result("300750", 530.0, 6.0)
        # 验证有两条结果记录
        count = portfolio.db.execute(
            "SELECT COUNT(*) FROM candidate_results WHERE symbol='300750'"
        ).fetchone()[0]
        assert count == 2
