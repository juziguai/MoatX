from modules.scoring_engine import ScoreBreakdown
from modules.stock_decision_report import StockDecisionReporter


class FakeScoringEngine:
    def __init__(self, score, risk):
        self._score = score
        self._risk = risk

    def score_single(self, symbol):
        return self._score

    def _get_risk_cached(self, symbol):
        return self._risk


class FakeEventDriver:
    def __init__(self, event):
        self._event = event

    def explain_single(self, symbol):
        return self._event


class FakeQuoteManager:
    def __init__(self, quote):
        self._quote = quote

    def fetch_quotes(self, symbols):
        symbol = symbols[0]
        return {f"{symbol}.SZ": self._quote}


class FakeAnnouncementScanner:
    def __init__(self, payload):
        self._payload = payload

    def scan(self, symbol):
        return self._payload


def _reporter(score, risk=None, event=None, quote=None, announcement=None):
    return StockDecisionReporter(
        scoring_engine=FakeScoringEngine(score, risk or {"risk_score": 0, "is_buyable": True}),
        event_driver=FakeEventDriver(event or {"symbol": score.symbol, "boost": 0.0, "reason": ""}),
        announcement_scanner=FakeAnnouncementScanner(
            announcement
            or {
                "symbol": score.symbol,
                "source": "cninfo",
                "risk_score": 0,
                "risk_level": "基本无风险",
                "is_buyable": True,
                "sentiment_score": 0,
                "red_flags": [],
                "positive_flags": [],
                "notices": [],
            }
        ),
        quote_manager=FakeQuoteManager(
            quote
            or {
                "name": "测试股份",
                "price": 10.0,
                "change_pct": 0.0,
                "pe": 20.0,
                "pb": 2.0,
                "turnover": 3.0,
            }
        ),
    )


def test_watch_stock_report_discourages_new_position_with_negative_event():
    score = ScoreBreakdown(
        symbol="002342",
        total=23.5,
        quality=8.0,
        timing=17.0,
        sentiment=0.0,
        event_multiplier=0.94,
        action="watch",
        suggested_weight=0.0,
        reasons=["KDJ深度超卖", "触及布林下轨"],
    )
    report = _reporter(
        score,
        event={"symbol": "002342", "boost": -6.0, "reason": "公告情绪(-6.0)"},
        quote={"name": "巨力索具", "price": 13.17, "change_pct": 2.97, "pe": 723.63, "pb": 5.17, "turnover": 27.84},
    ).build("002342")

    assert report["new_position"] == "不建议新开仓"
    assert "不建议现在买" in report["summary"]
    assert "不是加仓追" in report["summary"]
    assert any("PE 723.6" in item for item in report["key_points"])
    assert "一句话：" in report["markdown"]
    assert report["data_quality"]["confidence"] == "high"


def test_veto_or_high_risk_report_prioritizes_risk_release():
    score = ScoreBreakdown(
        symbol="600001",
        total=0.0,
        quality=0.0,
        timing=0.0,
        sentiment=0.0,
        action="no_buy",
        vetoed=True,
        veto_reason="高风险 40分",
    )
    report = _reporter(
        score,
        risk={"risk_score": 40, "risk_level": "中等风险", "is_buyable": False, "red_flags": ["监管立案"], "warnings": []},
    ).build("600001")

    assert report["risk_level"] == "high"
    assert report["new_position"] == "不建议新开仓"
    assert "风险暴露" in report["summary"]
    assert "监管立案" in report["markdown"]


def test_announcement_risk_can_veto_even_when_financial_risk_is_empty():
    score = ScoreBreakdown(
        symbol="002342",
        total=42.0,
        quality=18.0,
        timing=18.0,
        sentiment=4.0,
        action="probe",
        suggested_weight=0.05,
    )
    report = _reporter(
        score,
        risk={"risk_score": 0, "risk_level": "基本无风险", "is_buyable": True, "red_flags": [], "warnings": []},
        announcement={
            "symbol": "002342",
            "source": "cninfo",
            "risk_score": 35,
            "risk_level": "中等风险",
            "is_buyable": False,
            "sentiment_score": -20,
            "red_flags": ["[2026-05-15] 关于收到中国证券监督管理委员会立案告知书的公告"],
            "positive_flags": [],
            "notices": [],
        },
    ).build("002342")

    assert report["risk"]["risk_score"] == 35
    assert report["risk_level"] == "high"
    assert report["new_position"] == "不建议新开仓"
    assert "立案告知书" in report["markdown"]


def test_positive_high_score_report_allows_system_sized_participation():
    score = ScoreBreakdown(
        symbol="600519",
        total=76.0,
        quality=38.0,
        timing=24.0,
        sentiment=12.0,
        event_multiplier=1.05,
        action="heavy",
        suggested_weight=0.15,
    )
    report = _reporter(
        score,
        event={"symbol": "600519", "boost": 5.0, "reason": "事件(+5.0)"},
    ).build("600519")

    assert report["risk_level"] == "low"
    assert report["new_position"] == "可按系统仓位分批参与"
    assert "可以按系统仓位" in report["summary"]
