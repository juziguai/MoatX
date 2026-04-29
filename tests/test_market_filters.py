import pandas as pd

from modules.market_filters import (
    filter_selection_codes,
    filter_selection_universe,
    is_excluded_selection_board,
    market_board,
    normalize_code,
)


def test_board_detection_excludes_chinext_and_star():
    assert is_excluded_selection_board("300750")
    assert is_excluded_selection_board("301001")
    assert is_excluded_selection_board("688981")
    assert is_excluded_selection_board("689009")
    assert not is_excluded_selection_board("600111")
    assert not is_excluded_selection_board("000709")
    assert not is_excluded_selection_board("002079")


def test_market_board_labels():
    assert market_board("300750") == "ChiNext"
    assert market_board("688981") == "STAR"
    assert market_board("600111") == "Main"
    assert market_board("000709") == "Main"


def test_filter_selection_universe_detects_code_columns():
    df = pd.DataFrame(
        {
            "code": ["300750", "688981", "600111", "000709", "002079"],
            "name": ["宁德时代", "中芯国际", "北方稀土", "河钢股份", "苏州固锝"],
        }
    )

    result = filter_selection_universe(df)

    assert result["code"].tolist() == ["600111", "000709", "002079"]


def test_filter_selection_universe_supports_chinese_code_column():
    df = pd.DataFrame(
        {
            "代码": ["301001", "689009", "601899"],
            "名称": ["创业板样本", "科创板样本", "紫金矿业"],
        }
    )

    result = filter_selection_universe(df)

    assert result["代码"].tolist() == ["601899"]


def test_filter_selection_codes_normalizes_and_filters():
    assert filter_selection_codes(["300750", "sh.688981", "600111", "000709"]) == [
        "600111",
        "000709",
    ]
    assert normalize_code("sh.600111") == "600111"
