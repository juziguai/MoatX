"""K-fold 时序信号验证 — 防止单一时间窗口噪声导致的假信号。

将数据分为 k 个不重叠的回看窗口，每个窗口独立计算信号，
至少 consensus_threshold 比例窗口确认才接受信号。
保留时序顺序，不泄露未来数据。

借鉴自 JunHF/quant-trading-system 的 kfold_cross_validate。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass
class KFoldResult:
    """K-fold 验证结果"""
    consensus: bool          # 是否达到共识
    pass_count: int          # 确认信号的窗口数
    total: int               # 总窗口数
    ratio: float             # 通过比例
    details: list[dict]      # 每个窗口的详情


def kfold_validate(
    df: pd.DataFrame,
    signal_fn: Callable[[pd.DataFrame, int], bool],
    k: int = 3,
    consensus_threshold: float = 0.6,
    max_window: int = 20,
) -> KFoldResult:
    """对时序数据进行 K-fold 信号验证。

    Args:
        df: 包含 OHLCV 数据的 DataFrame
        signal_fn: 信号函数 (df_slice, fold_index) -> bool
        k: 折数
        consensus_threshold: 共识阈值（0.6 = 60% 窗口确认即通过）
        max_window: 每个窗口的最大长度（K 线数）

    Returns:
        KFoldResult 包含共识结果和每折详情
    """
    n = len(df)
    if n < k:
        return KFoldResult(consensus=False, pass_count=0, total=k, ratio=0.0,
                           details=[{"fold": i, "signal": False, "reason": "数据不足"} for i in range(k)])

    window_size = min(max_window, n // k)
    if window_size < 3:
        return KFoldResult(consensus=False, pass_count=0, total=k, ratio=0.0,
                           details=[{"fold": i, "signal": False, "reason": "窗口太小"} for i in range(k)])

    details = []
    pass_count = 0

    for i in range(k):
        end = n - i * window_size
        start = end - window_size
        if start < 0:
            start = 0
        if end <= start:
            details.append({"fold": i, "signal": False, "reason": "无数据"})
            continue

        fold_df = df.iloc[start:end].reset_index(drop=True)
        try:
            signal = signal_fn(fold_df, i)
        except Exception:
            signal = False

        if signal:
            pass_count += 1
        details.append({"fold": i, "signal": bool(signal), "start": start, "end": end})

    total = len(details)
    ratio = pass_count / total if total > 0 else 0.0
    consensus = ratio >= consensus_threshold

    return KFoldResult(
        consensus=consensus,
        pass_count=pass_count,
        total=total,
        ratio=ratio,
        details=details,
    )
