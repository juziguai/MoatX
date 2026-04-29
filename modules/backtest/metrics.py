"""Performance metrics calculation."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def calc_metrics(
    initial_capital: float,
    final_value: float,
    equity_curve: pd.DataFrame,
    trade_count: int = 0,
    risk_free_rate: float = 0.025,
) -> dict:
    """Calculate comprehensive performance metrics.

    Args:
        initial_capital: Starting capital
        final_value: Ending portfolio value
        equity_curve: DataFrame with columns ['date', 'total_value', 'returns']
        trade_count: Number of trades executed
        risk_free_rate: Annual risk-free rate (default 2.5%)

    Returns:
        dict of calculated metrics
    """
    if equity_curve.empty:
        return {"initial_capital": initial_capital, "final_value": final_value}

    df = equity_curve.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    total_return_pct = (final_value / initial_capital - 1) * 100

    days = (df["date"].iloc[-1] - df["date"].iloc[0]).days if "date" in df.columns else 0
    days = max(days, 1)
    years = days / 365.0
    annual_return_pct = ((final_value / initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    if "total_value" in df.columns:
        daily_returns = df["total_value"].pct_change().dropna().values
    elif "returns" in df.columns:
        daily_returns = np.diff(df["returns"].values)
    else:
        daily_returns = np.array([])

    sharpe_ratio = 0.0
    if len(daily_returns) > 1:
        excess_returns = daily_returns - risk_free_rate / 252
        std = np.std(daily_returns, ddof=1)
        sharpe_ratio = (np.mean(excess_returns) / std * math.sqrt(252)) if std > 0 else 0.0

    max_drawdown_pct = 0.0
    max_dd_start = None
    max_dd_end = None
    max_dd_recovery = None
    if "total_value" in df.columns:
        peak = df["total_value"].cummax()
        drawdown = (df["total_value"] - peak) / peak * 100
        if not drawdown.empty:
            min_idx = drawdown.idxmin()
            max_drawdown_pct = abs(drawdown.min())
            max_dd_start = str(df.loc[peak[:min_idx + 1].idxmax(), "date"].date()) if min_idx > 0 else str(df.iloc[0]["date"].date())
            max_dd_end = str(df.loc[min_idx, "date"].date())
            # Find recovery date
            post_trough = df.loc[min_idx + 1:, "total_value"]
            if not post_trough.empty:
                recovered = post_trough[post_trough >= peak[min_idx]]
                if not recovered.empty:
                    max_dd_recovery = str(recovered.index[0].date()) if hasattr(recovered.index[0], "date") else str(recovered.index[0])
                else:
                    max_dd_recovery = "未恢复"
            else:
                max_dd_recovery = "未恢复"
        dd_days = (df.loc[min_idx, "date"] - df.loc[peak[:min_idx + 1].idxmax(), "date"]).days if min_idx > 0 else 0
    else:
        dd_days = 0

    win_rate = 0.0
    profit_loss_ratio = 0.0
    if "total_value" in df.columns and len(df) > 1:
        daily_pnl = df["total_value"].diff().dropna()
        wins = daily_pnl[daily_pnl > 0]
        losses = daily_pnl[daily_pnl < 0]
        if trade_count > 0:
            win_rate = (len(wins) / max(trade_count, 1)) * 100
        if len(losses) > 0 and len(wins) > 0:
            avg_win = wins.mean()
            avg_loss = abs(losses.mean())
            profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0.0

    calmar_ratio = annual_return_pct / max_drawdown_pct if max_drawdown_pct > 0 else 0.0

    # Monthly returns
    monthly = []
    if "total_value" in df.columns and "date" in df.columns:
        df["month"] = df["date"].dt.to_period("M")
        monthly = (
            df.groupby("month")["total_value"]
            .agg(first="first", last="last")
            .assign(ret=lambda x: (x["last"] / x["first"] - 1) * 100)
            .reset_index()
        )
        monthly["month"] = monthly["month"].astype(str)
        monthly = monthly[["month", "ret"]].rename(columns={"ret": "return_pct"})
        monthly["return_pct"] = monthly["return_pct"].round(2).tolist()

    # Annual returns
    annual = []
    if "total_value" in df.columns and "date" in df.columns:
        df["year"] = df["date"].dt.year
        annual = (
            df.groupby("year")["total_value"]
            .agg(first="first", last="last")
            .assign(ret=lambda x: (x["last"] / x["first"] - 1) * 100)
            .reset_index()
        )
        annual["year"] = annual["year"].astype(int)
        annual["return_pct"] = annual["ret"].round(2)
        annual = annual[["year", "return_pct"]]

    return {
        "initial_capital": initial_capital,
        "final_value": final_value,
        "total_return_pct": round(total_return_pct, 2),
        "annual_return_pct": round(annual_return_pct, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "max_drawdown_start": max_dd_start,
        "max_drawdown_end": max_dd_end,
        "max_drawdown_recovery": max_dd_recovery,
        "max_drawdown_days": dd_days,
        "trade_count": trade_count,
        "win_rate": round(win_rate, 1),
        "profit_loss_ratio": round(profit_loss_ratio, 2),
        "calmar_ratio": round(calmar_ratio, 2),
        "monthly_returns": monthly,
        "annual_returns": annual,
    }
