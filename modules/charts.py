"""
charts.py - MoatX 可视化图表模块
K线、均线、MACD、KDJ、RSI、成交量
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.gridspec import GridSpec
from typing import Optional


class MoatXCharts:
    """MoatX 图表渲染器"""

    def __init__(self, df, symbol: str = ""):
        """
        Args:
            df: 包含 OHLCV + all_in_one() 指标的 DataFrame
            symbol: 股票代码
        """
        self.df = df
        self.symbol = symbol

    def plot(self, save_path: Optional[str] = None,
             style: str = "dark",
             figsize: tuple = (20, 16)):
        """
        渲染5面板完整图表

        Args:
            save_path: 可选，保存路径（如 "chart.png"）
            style: "dark"（深色）或 "light"（浅色）
            figsize: 图表尺寸
        """
        if style == "dark":
            self._apply_dark_style()
        else:
            self._apply_light_style()

        n = len(self.df)
        dates = self.df.index

        fig = plt.figure(figsize=figsize, facecolor="#1a1a2e" if style == "dark" else "white")
        gs = GridSpec(6, 1, height_ratios=[3, 1, 1, 1, 1, 1], hspace=0.05)

        ax1 = fig.add_subplot(gs[0])    # K线 + 布林带 + 均线
        ax2 = fig.add_subplot(gs[1], sharex=ax1)   # MACD
        ax3 = fig.add_subplot(gs[2], sharex=ax1)   # KDJ
        ax4 = fig.add_subplot(gs[3], sharex=ax1)   # RSI
        ax5 = fig.add_subplot(gs[4], sharex=ax1)   # 成交量
        ax_legend = fig.add_subplot(gs[5])           # 图例区（隐藏）
        ax_legend.axis("off")

        # 共享X轴设置
        for ax in [ax1, ax2, ax3, ax4, ax5]:
            ax.set_xlim(-1, n)

        # 5个面板
        self._plot_candlestick(ax1)
        self._plot_macd(ax2)
        self._plot_kdj(ax3)
        self._plot_rsi(ax4)
        self._plot_volume(ax5)

        # X轴日期标签（只在最底部显示）
        self._setup_x_axis(ax5, dates)

        # 图例
        self._plot_legend(ax_legend, style)

        # 总标题
        name = self.symbol
        fig.suptitle(f"{name}（{self.symbol}）技术分析",
                     fontsize=16, fontweight="bold",
                     color="#e0e0e0" if style == "dark" else "black",
                     y=0.95)

        plt.subplots_adjust(hspace=0.05, top=0.94, bottom=0.06)

        if save_path:
            fig.savefig(save_path, dpi=120, bbox_inches="tight",
                        facecolor=fig.get_facecolor())
            print(f"图表已保存: {save_path}")

        plt.show()

    def _plot_candlestick(self, ax):
        """K线 + 布林带 + 均线"""
        df = self.df
        n = len(df)

        # 逐一绘制K线
        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            is_up = c >= o
            body_bottom = min(o, c)
            body_height = abs(c - o) + 1e-9
            color = "#ef5350" if is_up else "#26a69a"

            # 实体
            rect = patches.Rectangle(
                (i - 0.35, body_bottom), 0.7, body_height,
                linewidth=0.5, edgecolor=color, facecolor=color, zorder=2
            )
            ax.add_patch(rect)
            # 上影线
            ax.plot([i, i], [h, max(o, c)], color=color, linewidth=0.8, zorder=1)
            # 下影线
            ax.plot([i, i], [min(o, c), l], color=color, linewidth=0.8, zorder=1)

        # 均线
        lw_ma = {"ma5": 1.0, "ma10": 1.0, "ma20": 1.0, "ma60": 1.0, "ma120": 1.0}
        color_ma = {"ma5": "#ff9800", "ma10": "#e040fb", "ma20": "#00bcd4",
                     "ma60": "#ffeb3b", "ma120": "#ff5722"}
        for ma_col, color in color_ma.items():
            if ma_col in df.columns:
                ax.plot(range(n), df[ma_col].values, color=color,
                        linewidth=lw_ma[ma_col], label=ma_col.upper(), zorder=3, alpha=0.9)

        # 布林带
        if "boll_upper" in df.columns:
            ax.plot(range(n), df["boll_upper"].values, color="#9e9e9e",
                    linewidth=0.8, linestyle="--", alpha=0.7, label="BOLL_U")
            ax.plot(range(n), df["boll_mid"].values, color="#9e9e9e",
                    linewidth=0.8, linestyle="--", alpha=0.7, label="BOLL_M")
            ax.plot(range(n), df["boll_lower"].values, color="#9e9e9e",
                    linewidth=0.8, linestyle="--", alpha=0.7, label="BOLL_L")
            ax.fill_between(range(n), df["boll_upper"].values, df["boll_lower"].values,
                            alpha=0.05, color="#9e9e9e")

        ax.set_ylabel("Price", fontsize=9)
        ax.tick_params(labelbottom=False)
        ax.legend(loc="upper left", fontsize=7, ncol=6, framealpha=0.3)
        ax.grid(True, alpha=0.15, linestyle="--")

    def _plot_macd(self, ax):
        """MACD 指标"""
        df = self.df
        n = len(df)

        dif = df["dif"].values
        dea = df["dea"].values
        macd = df["macd"].values

        # MACD 柱状图（红绿双色）
        colors = np.where(macd >= 0, "#ef5350", "#26a69a")
        ax.bar(range(n), macd, color=colors, width=0.7, alpha=0.8, label="MACD")
        ax.plot(range(n), dif, color="#2196f3", linewidth=1.2, label="DIF")
        ax.plot(range(n), dea, color="#ff9800", linewidth=1.2, label="DEA")
        ax.axhline(0, color="white", linewidth=0.5, alpha=0.5)
        ax.set_ylabel("MACD", fontsize=9)
        ax.tick_params(labelbottom=False)
        ax.legend(loc="upper left", fontsize=7, ncol=3, framealpha=0.3)
        ax.grid(True, alpha=0.15, linestyle="--")

    def _plot_kdj(self, ax):
        """KDJ 指标"""
        df = self.df
        n = len(df)

        ax.plot(range(n), df["k"].values, color="#2196f3", linewidth=1.2, label="K")
        ax.plot(range(n), df["d"].values, color="#ff9800", linewidth=1.2, label="D")
        ax.plot(range(n), df["j"].values, color="#e040fb", linewidth=1.0,
                linestyle="--", label="J", alpha=0.8)
        ax.axhline(80, color="#ef5350", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.axhline(20, color="#26a69a", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.set_ylabel("KDJ", fontsize=9)
        ax.tick_params(labelbottom=False)
        ax.legend(loc="upper left", fontsize=7, ncol=3, framealpha=0.3)
        ax.grid(True, alpha=0.15, linestyle="--")

    def _plot_rsi(self, ax):
        """RSI 指标"""
        df = self.df
        n = len(df)

        ax.plot(range(n), df["rsi6"].values, color="#2196f3", linewidth=1.0, label="RSI6", alpha=0.9)
        ax.plot(range(n), df["rsi12"].values, color="#ff9800", linewidth=1.2, label="RSI12")
        ax.plot(range(n), df["rsi24"].values, color="#e040fb", linewidth=1.0,
                linestyle="--", label="RSI24", alpha=0.8)
        ax.axhline(70, color="#ef5350", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.axhline(30, color="#26a69a", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.axhline(50, color="white", linewidth=0.5, alpha=0.3)
        ax.set_ylim(0, 100)
        ax.set_ylabel("RSI", fontsize=9)
        ax.tick_params(labelbottom=False)
        ax.legend(loc="upper left", fontsize=7, ncol=3, framealpha=0.3)
        ax.grid(True, alpha=0.15, linestyle="--")

    def _plot_volume(self, ax):
        """成交量柱状图（红涨绿跌）"""
        df = self.df
        n = len(df)

        colors = np.where(df["close"].values >= df["open"].values, "#ef5350", "#26a69a")
        ax.bar(range(n), df["volume"].values / 1e8, color=colors, width=0.7, alpha=0.7, label="Volume")

        if "vol_ma5" in df.columns:
            ax.plot(range(n), df["vol_ma5"].values / 1e8, color="#ff9800",
                    linewidth=1.0, label="Vol_MA5")
        if "vol_ma20" in df.columns:
            ax.plot(range(n), df["vol_ma20"].values / 1e8, color="#00bcd4",
                    linewidth=1.0, label="Vol_MA20", alpha=0.8)

        ax.set_ylabel("Volume(亿)", fontsize=9)
        ax.legend(loc="upper left", fontsize=7, ncol=3, framealpha=0.3)
        ax.grid(True, alpha=0.15, linestyle="--")

    def _setup_x_axis(self, ax, dates):
        """设置X轴日期标签"""
        n = len(dates)
        # 每隔一定天数显示一个标签
        step = max(1, n // 12)
        ticks = range(0, n, step)
        labels = [dates[i].strftime("%Y-%m-%d") if hasattr(dates[i], "strftime") else str(dates[i])[:10]
                  for i in ticks]
        ax.set_xticks(ticks)
        ax.set_xticklabels(labels, rotation=30, fontsize=8, ha="right")
        ax.set_xlabel("")

    def _plot_legend(self, ax, style="dark"):
        """图例面板"""
        ax.axis("off")
        text_color = "#e0e0e0" if style == "dark" else "#333333"
        legend_elements = [
            "K线: 红色=上涨 绿色=下跌",
            "MACD: DIF(蓝) DEA(橙) | 红柱=多头 绿柱=空头",
            "KDJ: K(蓝) D(橙) J(紫) | 80以上=超买 20以下=超卖",
            "RSI: RSI6(蓝) RSI12(橙) RSI24(紫) | 70以上=超买 30以下=超卖",
            "成交量: 红色=上涨 绿色=下跌",
        ]
        for i, text in enumerate(legend_elements):
            ax.text(0.01, 0.85 - i * 0.18, text, fontsize=9,
                    color=text_color, transform=ax.transAxes, verticalalignment="top")

    @staticmethod
    def _apply_dark_style():
        """深色主题配置"""
        plt.rcParams.update({
            "figure.facecolor": "#1a1a2e",
            "axes.facecolor": "#1a1a2e",
            "axes.edgecolor": "#333355",
            "axes.labelcolor": "#e0e0e0",
            "xtick.color": "#b0b0b0",
            "ytick.color": "#b0b0b0",
            "text.color": "#e0e0e0",
            "grid.color": "#333355",
            "grid.alpha": 0.3,
            "axes.titlesize": 11,
            "font.size": 9,
            "font.family": "Microsoft YaHei",
        })

    @staticmethod
    def _apply_light_style():
        """浅色主题配置"""
        plt.rcParams.update({
            "figure.facecolor": "white",
            "axes.facecolor": "#f8f8f8",
            "axes.edgecolor": "#cccccc",
            "axes.labelcolor": "#333333",
            "xtick.color": "#666666",
            "ytick.color": "#666666",
            "text.color": "#333333",
            "grid.color": "#cccccc",
            "grid.alpha": 0.5,
            "axes.titlesize": 11,
            "font.size": 9,
            "font.family": "Microsoft YaHei",
        })
