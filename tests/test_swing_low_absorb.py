import pandas as pd

from modules.swing_low_absorb import LowAbsorbSwingEngine


class FakeStockData:
    def __init__(self, spot, daily_map):
        self._spot = spot
        self._daily_map = daily_map

    def get_spot(self, use_cache=True):
        return self._spot

    def get_daily(self, symbol, *args, **kwargs):
        return self._daily_map[symbol]

    def check_financial_risk(self, symbol):
        return {"risk_score": 0, "risk_level": "基本无风险", "warnings": [], "is_buyable": True}


class FakeAnnouncementScanner:
    def scan(self, symbol, **kwargs):
        return {
            "risk_score": 0,
            "risk_level": "基本无风险",
            "is_buyable": True,
            "red_flags": [],
            "positive_flags": [],
        }


def _setup_daily(last_open=12.35, last_close=12.1, last_volume=18_000_000, impulse_volume=50_000_000):
    closes = [10 + i * 0.06 for i in range(35)]
    rows = []
    for i, close in enumerate(closes):
        rows.append(
            {
                "open": close * 0.995,
                "high": close * 1.02,
                "low": close * 0.99,
                "close": close,
                "volume": 1_600_000,
                "amount": close * 1_600_000,
                "turn": 2.0,
            }
        )
    rows[-8]["open"] = rows[-9]["close"] * 1.01
    rows[-8]["close"] = rows[-8]["open"] * 1.08
    rows[-8]["high"] = rows[-8]["close"] * 1.02
    rows[-8]["low"] = rows[-8]["open"] * 0.99
    rows[-8]["volume"] = impulse_volume
    rows[-8]["amount"] = rows[-8]["close"] * impulse_volume

    ma10 = pd.Series([row["close"] for row in rows]).rolling(10).mean().iloc[-1]
    rows[-1] = {
        "open": last_open,
        "high": max(last_open, last_close) * 1.01,
        "low": ma10 * 0.995,
        "close": last_close,
        "volume": last_volume,
        "amount": last_close * last_volume,
        "turn": 2.2,
    }
    return pd.DataFrame(rows, index=pd.date_range("2026-04-01", periods=len(rows), freq="B"))


def _breakout_daily():
    rows = []
    close = 20.0
    for i in range(34):
        close += 0.05
        rows.append(
            {
                "open": close * 0.995,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 10_000_000,
                "amount": close * 10_000_000,
                "turn": 2.0,
            }
        )
    prev_close = rows[-1]["close"]
    close = prev_close * 1.06
    rows.append(
        {
            "open": prev_close * 1.01,
            "high": close * 1.01,
            "low": prev_close * 0.995,
            "close": close,
            "volume": 18_000_000,
            "amount": close * 18_000_000,
            "turn": 4.0,
        }
    )
    return pd.DataFrame(rows, index=pd.date_range("2026-04-01", periods=len(rows), freq="B"))


def _momentum_pullback_daily():
    rows = []
    close = 50.0
    for _ in range(33):
        close += 0.25
        rows.append(
            {
                "open": close * 0.995,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 8_000_000,
                "amount": close * 8_000_000,
                "turn": 2.0,
            }
        )
    prev_close = rows[-1]["close"]
    strong_close = prev_close * 1.06
    rows.append(
        {
            "open": prev_close * 1.01,
            "high": strong_close * 1.02,
            "low": prev_close * 0.995,
            "close": strong_close,
            "volume": 16_000_000,
            "amount": strong_close * 16_000_000,
            "turn": 3.0,
        }
    )
    pullback_close = strong_close * 0.975
    rows.append(
        {
            "open": strong_close * 1.005,
            "high": strong_close * 1.005,
            "low": pullback_close * 0.975,
            "close": pullback_close,
            "volume": 14_000_000,
            "amount": pullback_close * 14_000_000,
            "turn": 2.8,
        }
    )
    return pd.DataFrame(rows, index=pd.date_range("2026-04-01", periods=len(rows), freq="B"))


def _strong_trend_daily():
    rows = []
    close = 36.0
    for _ in range(32):
        close *= 1.006
        rows.append(
            {
                "open": close * 0.995,
                "high": close * 1.012,
                "low": close * 0.99,
                "close": close,
                "volume": 12_000_000,
                "amount": close * 12_000_000,
                "turn": 2.0,
            }
        )
    for pct, volume in [(1.100, 30_000_000), (1.099, 36_000_000), (1.065, 20_000_000)]:
        prev_close = close
        close *= pct
        rows.append(
            {
                "open": prev_close * 1.01,
                "high": close * 1.015,
                "low": prev_close * 0.995,
                "close": close,
                "volume": volume,
                "amount": close * volume,
                "turn": 5.0,
            }
        )
    return pd.DataFrame(rows, index=pd.date_range("2026-04-01", periods=len(rows), freq="B"))


def _repeating_breakout_daily():
    rows = []
    close = 20.0
    for i in range(70):
        if i in {30, 45, 60, 69}:
            prev_close = close
            close = prev_close * 1.06
            rows.append(
                {
                    "open": prev_close * 1.01,
                    "high": close * 1.01,
                    "low": prev_close * 0.995,
                    "close": close,
                    "volume": 25_000_000,
                    "amount": close * 25_000_000,
                    "turn": 4.0,
                }
            )
        elif i in {31, 46, 61}:
            prev_close = close
            close = prev_close * 1.02
            rows.append(
                {
                    "open": prev_close * 1.002,
                    "high": prev_close * 1.025,
                    "low": prev_close * 0.995,
                    "close": close,
                    "volume": 12_000_000,
                    "amount": close * 12_000_000,
                    "turn": 2.0,
                }
            )
        else:
            close *= 1.002
            rows.append(
                {
                    "open": close * 0.995,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "volume": 8_000_000,
                    "amount": close * 8_000_000,
                    "turn": 1.5,
                }
            )
    return pd.DataFrame(rows, index=pd.date_range("2026-02-02", periods=len(rows), freq="B"))


def test_analyze_scores_low_absorb_candidate():
    daily = _setup_daily()
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001", name="Test Stock")

    assert plan["score"] >= 55
    assert plan["action"] in {"candidate", "watch"}
    assert plan["plan"]["entry_low"] < plan["plan"]["target_1"] < plan["plan"]["target_2"]
    assert plan["plan"]["stop_loss"] < plan["plan"]["entry_low"]
    assert plan["metrics"]["stop_loss_pct"] >= -2.1
    assert any("缩量" in reason for reason in plan["reasons"])


def test_analyze_rejects_chasing_big_positive_day():
    daily = _setup_daily(last_open=12.0, last_close=12.6)
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001")

    assert plan["action"] != "candidate"
    assert any("追涨" in warning for warning in plan["warnings"])


def test_analyze_flags_breakout_ignition_watch():
    daily = _breakout_daily()
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001")

    assert plan["setup"] == "放量突破首日"
    assert plan["score"] >= 55
    assert plan["action"] in {"candidate", "watch"}
    assert any("突破" in reason for reason in plan["reasons"])


def test_analyze_keeps_momentum_pullback_watch():
    daily = _momentum_pullback_daily()
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001")

    assert plan["score"] >= 55
    assert plan["action"] in {"candidate", "watch"}
    assert any("强势回踩" in reason for reason in plan["reasons"])


def test_strong_trend_survives_weak_market_as_watch():
    daily = _strong_trend_daily()
    spot = pd.DataFrame(
        {
            "code": [f"600{i:03d}" for i in range(1, 401)],
            "name": [f"S{i}" for i in range(1, 401)],
            "price": [12.1] * 400,
            "pct_change": [-2.0] * 400,
            "amount": [100_000_000] * 400,
        }
    )
    market_context = LowAbsorbSwingEngine._market_context(spot)
    sector_context = {
        "tags_by_code": {"600001": {"机器人"}},
        "boards": {"机器人": {"pct_change": -3.5}},
    }
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001", market_context=market_context, sector_context=sector_context)

    assert market_context["state"] == "severe"
    assert plan["setup"] == "强趋势延续观察"
    assert plan["score"] >= 55
    assert plan["action"] == "watch"
    assert any("强趋势" in reason for reason in plan["reasons"])


def test_historical_reference_adjusts_breakout_score():
    daily = _repeating_breakout_daily()
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001", check_risk=False)
    history = plan["metrics"]["historical_reference"]

    assert plan["setup"] == "放量突破首日"
    assert history["sample_count"] >= 3
    assert history["target_hit_rate_pct"] >= 60
    assert history["score_adjust"] > 0
    assert any("历史相似" in reason for reason in plan["reasons"])


def test_analyze_preserves_trend_profile_before_low_absorb_hard_skip():
    daily = _setup_daily(last_open=12.0, last_close=12.6)
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )
    engine._trend_continuation_profile = lambda **kwargs: {
        "score": 54.0,
        "setup": "强趋势延续观察",
        "exit_rule": "趋势退出规则",
        "reasons": ["强趋势测试"],
        "warnings": ["趋势风险测试"],
    }
    engine._breakout_ignition_profile = lambda **kwargs: None

    plan = engine.analyze("600001", check_risk=False)

    assert plan["setup"] == "强趋势延续观察"
    assert plan["score"] == 54.0
    assert plan["warnings"][0] == "趋势风险测试"


def test_strong_trend_scores_near_high_in_segments():
    daily = _strong_trend_daily()
    daily.iloc[-1, daily.columns.get_loc("high")] = daily.iloc[-1]["close"] / 0.965
    spot = pd.DataFrame(
        {
            "code": [f"600{i:03d}" for i in range(1, 401)],
            "name": [f"S{i}" for i in range(1, 401)],
            "price": [12.1] * 400,
            "pct_change": [-2.0] * 400,
            "amount": [100_000_000] * 400,
        }
    )
    market_context = LowAbsorbSwingEngine._market_context(spot)
    sector_context = {
        "tags_by_code": {"600001": {"机器人"}},
        "boards": {"机器人": {"pct_change": -3.5}},
    }
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001", market_context=market_context, sector_context=sector_context)

    assert plan["setup"] == "强趋势延续观察"
    assert plan["action"] == "watch"
    assert any("20日高位区" in reason for reason in plan["reasons"])


def test_candidates_filters_excluded_boards_and_sorts():
    spot = pd.DataFrame(
        {
            "code": ["600001", "300001", "002001"],
            "name": ["A", "B", "C"],
            "price": [12.1, 20.0, 12.1],
            "pct_change": [-0.5, -0.8, -0.6],
            "amount": [200_000_000, 300_000_000, 180_000_000],
            "turnover": [2.0, 3.0, 2.5],
        }
    )
    daily = _setup_daily()
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {"600001": daily, "002001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    rows = engine.candidates(limit=5, pool_limit=5, workers=2)

    assert {row["symbol"] for row in rows} <= {"600001", "002001"}
    assert "300001" not in {row["symbol"] for row in rows}
    assert rows == sorted(rows, key=lambda row: row["score"], reverse=True)


def test_prefilter_rank_blends_strength_before_amount():
    spot = pd.DataFrame(
        {
            "code": ["600001", "600002", "600003"],
            "name": ["Large Weak", "Strong", "Absorb"],
            "price": [10.0, 20.0, 12.0],
            "pct_change": [-5.0, 6.8, -0.5],
            "amount": [500_000_000, 300_000_000, 200_000_000],
            "turnover": [0.5, 12.0, 5.0],
        }
    )

    ranked = LowAbsorbSwingEngine._rank_prefilter_pool(spot, {})

    assert ranked.iloc[0]["code"] == "600002"


def test_prefilter_pool_keeps_liquidity_guard():
    spot = pd.DataFrame(
        {
            "code": ["600001", "600002", "600003"],
            "name": ["Large Weak", "Strong", "Absorb"],
            "price": [10.0, 20.0, 12.0],
            "pct_change": [-5.0, 6.8, -0.5],
            "amount": [500_000_000, 300_000_000, 200_000_000],
            "turnover": [0.5, 12.0, 5.0],
        }
    )

    ranked = LowAbsorbSwingEngine._rank_prefilter_pool(spot, {})
    selected = LowAbsorbSwingEngine._select_prefilter_pool(ranked, pool_limit=1)

    assert {"600001", "600002"} <= set(selected["code"])


def test_candidates_prefilter_keeps_strong_lower_amount_stock():
    spot = pd.DataFrame(
        {
            "code": ["600001", "600002"],
            "name": ["Large Weak", "Strong"],
            "price": [10.0, 20.0],
            "pct_change": [-5.0, 6.8],
            "amount": [500_000_000, 300_000_000],
            "turnover": [0.5, 12.0],
        }
    )
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {"600002": _strong_trend_daily()}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )
    engine._quote_snapshot = lambda symbols: {}

    rows = engine.candidates(limit=5, pool_limit=1, workers=1)

    assert [row["symbol"] for row in rows] == ["600002"]


def test_spot_snapshot_falls_back_to_stale_cache(monkeypatch):
    cached_spot = pd.DataFrame({"code": ["600001"], "pct_change": [-1.0]})

    class CacheResult:
        ok = False
        error = "expired"
        data = cached_spot

    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    monkeypatch.setattr(engine._sd, "get_spot", lambda use_cache=True: pd.DataFrame())
    monkeypatch.setattr("modules.crawler.cache.read_df_cache", lambda *args, **kwargs: CacheResult())

    assert engine._spot_snapshot().equals(cached_spot)


def test_market_breadth_gate_demotes_weak_market():
    daily = _setup_daily()
    spot = pd.DataFrame(
        {
            "code": [f"600{i:03d}" for i in range(1, 401)],
            "name": [f"S{i}" for i in range(1, 401)],
            "price": [12.1] * 400,
            "pct_change": [-2.0] * 400,
            "amount": [100_000_000] * 400,
        }
    )
    market_context = LowAbsorbSwingEngine._market_context(spot)
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {"600001": daily}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )

    plan = engine.analyze("600001", market_context=market_context)

    assert market_context["state"] == "severe"
    assert plan["action"] != "candidate"
    assert any("大盘宽度" in warning for warning in plan["warnings"])


def test_build_paper_account_uses_equal_cash_not_fixed_shares():
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(pd.DataFrame(), {}),
        announcement_scanner=FakeAnnouncementScanner(),
        enable_sector_context=False,
    )
    rows = [
        {
            "symbol": "601869",
            "name": "High Price",
            "score": 79.0,
            "action": "candidate",
            "trade_date": "2026-05-25",
            "metrics": {"close": 386.03},
            "plan": {"target_1": 391.82, "target_2": 397.61, "stop_loss": 378.31},
            "reasons": [],
            "warnings": [],
        },
        {
            "symbol": "002241",
            "name": "Low Price",
            "score": 72.0,
            "action": "candidate",
            "trade_date": "2026-05-25",
            "metrics": {"close": 25.49},
            "plan": {"target_1": 25.87, "target_2": 26.25, "stop_loss": 24.98},
            "reasons": [],
            "warnings": [],
        },
    ]

    account = engine.build_paper_account(rows, cash_per_stock=10_000, lot_size=100)

    assert [row["symbol"] for row in account["positions"]] == ["002241"]
    assert account["positions"][0]["quantity"] == 300
    assert account["skipped"][0]["symbol"] == "601869"


def test_sector_graph_covers_failed_swing_names():
    spot = pd.DataFrame(
        {
            "code": ["601869", "600498", "002241"],
            "name": ["A", "B", "C"],
            "pct_change": [1.0, 1.0, 1.0],
        }
    )
    engine = LowAbsorbSwingEngine(
        stock_data=FakeStockData(spot, {}),
        announcement_scanner=FakeAnnouncementScanner(),
    )

    context = engine._sector_context(["601869", "600498", "002241"], spot=spot)

    assert "光通信" in context["tags_by_code"]["601869"]
    assert "光通信" in context["tags_by_code"]["600498"]
    assert "消费电子" in context["tags_by_code"]["002241"]
