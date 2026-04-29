"""Market-board filters shared by stock selection pipelines."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


EXCLUDED_SELECTION_PREFIXES: tuple[str, ...] = ("300", "301", "688", "689")
EXCLUDED_SELECTION_MARKETS: tuple[str, ...] = ("ChiNext", "STAR")


def normalize_code(value: object) -> str:
    """Return a six-digit A-share code when possible."""
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[-1] if text.lower().startswith(("sh.", "sz.")) else text.split(".")[0]
    text = "".join(ch for ch in text if ch.isdigit())
    return text.zfill(6) if text else ""


def market_board(code: object) -> str:
    """Classify an A-share code into a coarse market board."""
    normalized = normalize_code(code)
    if normalized.startswith(("300", "301")):
        return "ChiNext"
    if normalized.startswith(("688", "689")):
        return "STAR"
    if normalized.startswith(("600", "601", "603", "605", "000", "001", "002", "003")):
        return "Main"
    if normalized.startswith(("8", "4", "9")):
        return "BSE"
    return "Other"


def is_excluded_selection_board(code: object) -> bool:
    """Whether a stock should be excluded from MoatX buy-selection output."""
    return normalize_code(code).startswith(EXCLUDED_SELECTION_PREFIXES)


def _detect_code_column(df: pd.DataFrame, preferred: str | None = None) -> str | None:
    if preferred and preferred in df.columns:
        return preferred
    for column in ("code", "symbol", "代码", "证券代码", "股票代码"):
        if column in df.columns:
            return column
    return None


def filter_selection_universe(
    df: pd.DataFrame,
    code_col: str | None = None,
    excluded_prefixes: Iterable[str] = EXCLUDED_SELECTION_PREFIXES,
) -> pd.DataFrame:
    """Remove ChiNext/STAR stocks from a candidate DataFrame.

    The filter is intentionally code-prefix based so it works before any
    industry or exchange metadata has been fetched.
    """
    if df is None or df.empty:
        return df
    column = _detect_code_column(df, code_col)
    if not column:
        return df
    prefixes = tuple(str(prefix) for prefix in excluded_prefixes)
    mask = ~df[column].map(lambda value: normalize_code(value).startswith(prefixes))
    return df.loc[mask].copy()


def filter_selection_codes(codes: Iterable[object]) -> list[str]:
    """Return normalized codes excluding ChiNext and STAR."""
    return [
        code
        for code in (normalize_code(item) for item in codes)
        if code and not is_excluded_selection_board(code)
    ]
