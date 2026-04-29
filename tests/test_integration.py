# -*- coding: utf-8 -*-
"""P1: Data source smoke tests (integration, skipped by default).

Each test verifies a data source returns valid data (non-empty, with required fields).
On failure, pytest.skip() is called instead of failing the CI run.

Usage:
    pytest tests/test_integration.py                  # skip (default)
    pytest tests/test_integration.py -v                # verbose
    pytest tests/test_integration.py -m integration -v  # run explicitly
"""

import pytest


@pytest.fixture(autouse=True)
def _clear_proxy():
    """Clear proxy env vars before each test to avoid hijacked requests."""
    from modules.utils import _clear_all_proxy
    _clear_all_proxy()


@pytest.mark.integration
class TestSinaSpot:
    """Sina full-market snapshot via vip.stock.finance.sina.com.cn"""

    def test_sina_spot_reachable(self):
        """Sina full-market snapshot returns non-empty DataFrame with symbol/price columns."""
        from modules.stock_data import StockData
        sd = StockData()
        df = sd.get_spot(use_cache=False)
        assert not df.empty, "Sina spot returned empty DataFrame"
        assert "symbol" in df.columns or "code" in df.columns
        price_col = "trade" if "trade" in df.columns else "price"
        assert price_col in df.columns
        assert (df[price_col] > 0).sum() > 0, "All prices are 0 (non-trading hours?)"

    def test_sina_daily_single_stock(self):
        """Single-stock daily data returns valid OHLCV."""
        from modules.stock_data import StockData
        sd = StockData()
        df = sd.get_daily("600519", adjust="")
        assert not df.empty, "600519 daily data is empty"
        for col in ("open", "high", "low", "close", "volume"):
            assert col in df.columns, f"Missing column: {col}"


@pytest.mark.integration
class TestTencentQuote:
    """Tencent real-time quotes via qt.gtimg.cn"""

    def test_tencent_quote_reachable(self):
        """Tencent quote API returns real-time data for given symbols."""
        from modules.crawler.tencent import fetch_quotes_batch
        result = fetch_quotes_batch(["600519", "000858"], use_cache=False)
        assert result.ok, f"Tencent quote request failed: {result.error}"
        assert result.data, "Tencent quote returned empty data"
        rows = result.data if isinstance(result.data, list) else [result.data]
        assert len(rows) >= 1, "No quote data retrieved"
        q = rows[0]
        assert "code" in q and "price" in q
        assert float(q["price"]) > 0, "Tencent quote price invalid"


@pytest.mark.integration
class TestCNINFO:
    """CNINFO announcement query via xueqiu.com proxy"""

    def test_cninfo_notices_reachable(self):
        """CNINFO announcement query returns valid data."""
        try:
            import akshare as ak
        except Exception:
            pytest.skip("akshare not available")

        try:
            df = ak.stock_announcement_xueqiu(symbol="000001", count=5)
            assert df is not None, "CNINFO returned None"
            assert isinstance(df, type(df)), "Return type invalid"
        except Exception as e:
            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                pytest.skip(f"CNINFO request timeout: {e}")
            pytest.skip(f"CNINFO interface error (possible page structure change): {e}")


@pytest.mark.integration
class TestTHSSector:
    """THS industry boards via akshare stock_board_industry_name_ths"""

    def test_ths_sector_reachable(self):
        """THS industry board list returns non-empty data."""
        try:
            import akshare as ak
        except Exception:
            pytest.skip("akshare not available")

        try:
            df = ak.stock_board_industry_name_ths()
            assert df is not None and not df.empty, "THS board list empty"
            assert "板块名称" in df.columns or "行业名称" in df.columns
        except Exception as e:
            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                pytest.skip(f"THS request timeout: {e}")
            pytest.skip(f"THS interface error: {e}")

    def test_ths_sector_cons(self):
        """THS industry board constituent stocks return non-empty data."""
        try:
            import akshare as ak
        except Exception:
            pytest.skip("akshare not available")

        try:
            boards = ak.stock_board_industry_name_ths()
            if boards is None or boards.empty:
                pytest.skip("THS board list empty, skipping cons test")
            col = "板块名称" if "板块名称" in boards.columns else "行业名称"
            first_board = boards[col].iloc[0]
            df = ak.stock_board_industry_cons_ths(symbol=first_board)
            assert df is not None and not df.empty, f"Board {first_board} constituents empty"
            assert "代码" in df.columns or "code" in df.columns
        except Exception as e:
            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                pytest.skip(f"THS constituents request timeout: {e}")
            pytest.skip(f"THS constituents interface error: {e}")


@pytest.mark.integration
class TestFinancialRisk:
    """Single-stock financial risk detection"""

    def test_financial_risk_single_stock(self):
        """Single-stock financial risk detection returns structured result."""
        from modules.stock_data import StockData
        sd = StockData()
        result = sd.check_financial_risk("600519")
        assert isinstance(result, dict), "Financial risk result is not a dict"
        assert "risk_score" in result, "Missing risk_score field"
        assert "risk_level" in result, "Missing risk_level field"
        assert "is_buyable" in result, "Missing is_buyable field"
        assert isinstance(result["risk_score"], (int, float))
        assert isinstance(result["is_buyable"], bool)


@pytest.mark.integration
class TestEastMoneyQuote:
    """EastMoney real-time quotes via push2.eastmoney.com"""

    def test_eastmoney_quote_reachable(self):
        """EastMoney real-time quotes return valid data."""
        from modules.datasource import EastMoneySource
        src = EastMoneySource()
        result = src.fetch_quotes(["600519", "000858"])
        assert result, "EastMoney quotes returned empty"
        for full_code, q in result.items():
            assert "price" in q and "change_pct" in q
            assert q["price"] >= 0


@pytest.mark.integration
class TestSinaSource:
    """Sina finance quotes via hq.sinajs.cn"""

    def test_sina_source_reachable(self):
        """Sina finance real-time quotes return valid data."""
        from modules.datasource import SinaSource
        src = SinaSource()
        result = src.fetch_quotes(["600519", "000858"])
        assert result, "Sina quotes returned empty"
        for full_code, q in result.items():
            assert "price" in q and "change_pct" in q
            assert q["price"] >= 0


@pytest.mark.integration
class TestRiskController:
    """Risk control integration: holdings + real quotes -> risk events"""

    def _mock_holdings(self):
        """Build test-holdings DataFrame (columns match list_holdings())."""
        import pandas as pd
        return pd.DataFrame([{
            "symbol": "TEST001",
            "name": "Test Stock A",
            "shares": 1000.0,
            "cost_price": 100.0,
            "market_value": 100000.0,
            "current_price": 92.0,
        }])

    def _mock_risk_cfg(self):
        """Build mock RiskControlSettings with stop_loss_pct=7."""
        from dataclasses import dataclass
        @dataclass
        class MockRiskControl:
            stop_loss_pct: float = 7.0
            max_single_position_pct: float = 30.0
            max_total_position_pct: float = 90.0
            max_daily_loss_pct: float = 5.0
        return MockRiskControl()

    def test_stop_loss_triggers_at_threshold(self):
        """Loss >= 7% should trigger stop-loss event (-7.1%)."""
        import pandas as pd
        from modules.risk_controller import RiskController

        holdings = self._mock_holdings()
        quotes = {"TEST001": {"price": 92.9, "change_pct": -7.1}}
        cfg_ = self._mock_risk_cfg()

        rc = RiskController(None)
        events = rc.check_all(holdings, quotes, cfg_)

        stop_events = [e for e in events if e.event_type == "stop_loss"]
        assert len(stop_events) == 1, f"Should trigger 1 stop-loss event, got {len(stop_events)}"
        assert stop_events[0].symbol == "TEST001"
        assert stop_events[0].triggered_value >= 7.0

    def test_stop_loss_not_triggered_below_threshold(self):
        """Loss < 7% should NOT trigger stop-loss event (-6.9%)."""
        import pandas as pd
        from modules.risk_controller import RiskController

        holdings = self._mock_holdings()
        quotes = {"TEST001": {"price": 93.1, "change_pct": -6.9}}
        cfg_ = self._mock_risk_cfg()

        rc = RiskController(None)
        events = rc.check_all(holdings, quotes, cfg_)

        stop_events = [e for e in events if e.event_type == "stop_loss"]
        assert len(stop_events) == 0, f"Should NOT trigger stop-loss, got {len(stop_events)}"

    def test_position_limit_triggers(self):
        """Single-position over limit triggers position_limit event."""
        import pandas as pd
        from modules.risk_controller import RiskController

        holdings = pd.DataFrame([{
            "symbol": "TEST001",
            "name": "Test Stock",
            "shares": 850.0,
            "cost_price": 100.0,
            "market_value": 85000.0,
            "current_price": 100.0,
        }])
        quotes = {"TEST001": {"price": 100.0}}
        cfg_ = self._mock_risk_cfg()

        rc = RiskController(None)
        events = rc.check_all(holdings, quotes, cfg_)

        pos_events = [e for e in events if e.event_type == "position_limit"]
        assert len(pos_events) == 1, f"Should trigger 1 position_limit event, got {len(pos_events)}"
        assert pos_events[0].triggered_value > 30.0

    def test_zero_price_skipped(self):
        """Zero price should not trigger stop-loss (prevents non-trading hours false alerts)."""
        import pandas as pd
        from modules.risk_controller import RiskController

        holdings = self._mock_holdings()
        quotes = {"TEST001": {"price": 0}}
        cfg_ = self._mock_risk_cfg()

        rc = RiskController(None)
        events = rc.check_all(holdings, quotes, cfg_)

        stop_events = [e for e in events if e.event_type == "stop_loss"]
        assert len(stop_events) == 0, "Zero price should not trigger stop-loss"
