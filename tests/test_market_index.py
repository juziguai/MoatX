from modules.market_index import (
    IndexQuote,
    MarketBreadth,
    MarketIndexQuoteManager,
    fetch_sina_market_breadth,
    _sina_count_by_boundary,
    normalize_index_codes,
)


def test_normalize_index_codes_supports_aliases():
    assert normalize_index_codes(["上证指数", "399001", "创业板指", "科创50", "北证50"]) == [
        "sh000001",
        "sz399001",
        "sz399006",
        "sh000688",
        "bj899050",
    ]


def test_aggregate_marks_verified_when_sources_close():
    manager = MarketIndexQuoteManager(tolerance_pct=0.1)
    row = manager._aggregate(
        "sh000001",
        [
            IndexQuote(
                code="sh000001",
                name="上证指数",
                price=4087.2,
                prev_close=4079.9,
                change=7.3,
                pct_change=0.18,
                datetime="2026-04-27 10:03:21",
                source="tencent",
            ),
            IndexQuote(
                code="sh000001",
                name="上证指数",
                price=4085.13,
                prev_close=4079.9,
                change=5.23,
                pct_change=0.13,
                datetime="2026-04-27 10:04:29",
                source="sina",
            ),
        ],
    )

    assert row.status == "verified"
    assert row.sources == ["sina", "tencent"]
    assert row.max_pct_diff == 0.05


def test_aggregate_marks_diverged_when_sources_disagree():
    manager = MarketIndexQuoteManager(tolerance_pct=0.1)
    row = manager._aggregate(
        "sh000001",
        [
            IndexQuote(
                code="sh000001",
                name="上证指数",
                price=4087.2,
                prev_close=4079.9,
                change=7.3,
                pct_change=0.18,
                datetime="2026-04-27 10:03:21",
                source="tencent",
            ),
            IndexQuote(
                code="sh000001",
                name="上证指数",
                price=4100.0,
                prev_close=4079.9,
                change=20.1,
                pct_change=0.49,
                datetime="2026-04-27 10:04:29",
                source="sina",
            ),
        ],
    )

    assert row.status == "diverged"
    assert row.warning


def test_sina_boundary_count_positive_and_negative(monkeypatch):
    pages = {
        (1, False): [5, 4, 3],
        (2, False): [2, 1, 0],
        (3, False): [0, -1, -2],
        (1, True): [-5, -4, -3],
        (2, True): [-2, -1, 0],
        (3, True): [0, 1, 2],
    }

    def fake_page(page, asc, timeout=None, page_size=3):
        return pages[(page, asc)]

    monkeypatch.setattr("modules.market_index._sina_sorted_pct_page", fake_page)

    assert _sina_count_by_boundary(total=9, positive=True, page_size=3) == 5
    assert _sina_count_by_boundary(total=9, positive=False, page_size=3) == 5


def test_sina_breadth_falls_back_when_boundary_counts_invalid(monkeypatch):
    monkeypatch.setattr("modules.market_index._sina_stock_count", lambda timeout=None: 10)
    monkeypatch.setattr("modules.market_index._sina_count_by_boundary", lambda *args, **kwargs: 8)

    def fake_full_scan(total_hint, timeout=None, page_size=100, started=None):
        return MarketBreadth(
            total=10,
            up=4,
            down=5,
            flat=1,
            datetime="2026-04-27 11:00:00",
            source="sina",
            method="full_scan",
            elapsed=0.1,
        )

    monkeypatch.setattr("modules.market_index._sina_market_breadth_full_scan", fake_full_scan)

    row = fetch_sina_market_breadth(page_size=3)

    assert row.method == "full_scan"
    assert (row.up, row.down, row.flat) == (4, 5, 1)
    assert "回退全量扫描" in row.warning
