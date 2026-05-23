import sys
import types

import pandas as pd

try:
    import akshare  # noqa: F401
except Exception:
    sys.modules["akshare"] = types.SimpleNamespace()

from modules.scoring_engine import ScoringEngine, _event_multiplier_from_boost


class FakeStockData:
    def __init__(self, spot):
        self._spot = spot

    def get_spot(self):
        return self._spot


def _engine_with_no_external_calls(spot):
    engine = ScoringEngine()
    engine._sd = FakeStockData(spot)
    engine._detect_regime = lambda: "neutral"
    engine._score_timing_batch = lambda df, regime: df.assign(timing=0.0)
    engine._score_sentiment_batch = lambda df, regime: df.assign(sentiment=0.0)
    engine._apply_event_multiplier = lambda df: df.assign(event_multiplier=1.0)
    engine._apply_concentration_penalty = lambda df, holdings: df
    return engine


def test_cheapness_score_rewards_lower_valuation():
    market = pd.Series([5.0, 10.0, 20.0, 40.0])

    low_pe_score, _ = ScoringEngine._cheapness_score(market, 5.0, 15)
    high_pe_score, _ = ScoringEngine._cheapness_score(market, 40.0, 15)

    assert low_pe_score > high_pe_score


def test_event_multiplier_from_boost_is_capped():
    assert _event_multiplier_from_boost(40) == 1.3
    assert _event_multiplier_from_boost(30) == 1.3
    assert _event_multiplier_from_boost(-80) == 0.6
    assert _event_multiplier_from_boost(12.5) == 1.125


def test_score_batch_all_veto_keeps_output_protocol():
    spot = pd.DataFrame(
        {
            "code": ["600001", "600002"],
            "pe": [10.0, 20.0],
            "pb": [1.0, 2.0],
        }
    )
    engine = _engine_with_no_external_calls(spot)
    engine._check_veto = lambda symbol: (True, "risk")

    candidates = pd.DataFrame(
        {
            "code": ["600001", "600002"],
            "name": ["A", "B"],
            "price": [10.0, 20.0],
            "pe": [10.0, 20.0],
            "pb": [1.0, 2.0],
            "turnover": [1.0, 1.0],
        }
    )

    scored = engine.score_batch(candidates)

    for col in ["quality", "timing", "sentiment", "event_multiplier", "total", "action", "suggested_weight"]:
        assert col in scored.columns
    assert scored["total"].tolist() == [0.0, 0.0]
    assert scored["action"].tolist() == ["no_buy", "no_buy"]
    assert scored["suggested_weight"].tolist() == [0.0, 0.0]


def test_score_batch_partial_veto_has_no_nan_protocol_fields():
    spot = pd.DataFrame(
        {
            "code": ["600001", "600002"],
            "pe": [8.0, 80.0],
            "pb": [1.0, 8.0],
        }
    )
    engine = _engine_with_no_external_calls(spot)
    engine._check_veto = lambda symbol: (symbol == "600002", "risk" if symbol == "600002" else "")
    engine._score_profitability_batch = lambda df, active_idx: pd.Series({idx: 0.0 for idx in active_idx})

    candidates = pd.DataFrame(
        {
            "code": ["600001", "600002"],
            "name": ["LowPE", "Vetoed"],
            "price": [10.0, 20.0],
            "pe": [8.0, 80.0],
            "pb": [1.0, 8.0],
            "turnover": [1.0, 1.0],
        }
    )

    scored = engine.score_batch(candidates)

    protocol_cols = ["quality", "timing", "sentiment", "event_multiplier", "total", "action", "suggested_weight"]
    assert not scored[protocol_cols].isna().any().any()
    vetoed = scored[scored["code"] == "600002"].iloc[0]
    assert vetoed["total"] == 0.0
    assert vetoed["action"] == "no_buy"
