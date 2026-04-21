"""
analyzer.py - MoatX 核心分析引擎
把数据获取 + 指标计算 + 信号判断 + 报告生成 整合在一起
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Literal

from .stock_data import StockData
from .indicators import IndicatorEngine
from .charts import MoatXCharts
from .screener import MoatXScreener
from .rank_engine import RankEngine


class MoatXAnalyzer:
    """
    MoatX A股分析器
    使用方法:
        analyzer = MoatXAnalyzer()
        report = analyzer.analyze("600519")
        print(report)
    """

    def __init__(self):
        self.data = StockData()
        self.ind = IndicatorEngine()

    def analyze(
        self,
        symbol: str,
        days: int = 120,
        adjust: Literal["qfq", "hfq", ""] = "qfq"
    ) -> dict:
        """
        综合分析一只股票

        Returns:
            dict: 包含实时行情、技术指标、信号判断、风险评估
        """
        # 获取数据
        df = self.data.get_daily(symbol, adjust=adjust)
        df = df.tail(days)

        # 计算指标
        ind_df = self.ind.all_in_one(df)
        df = pd.concat([df, ind_df], axis=1)

        # 获取实时行情
        try:
            realtime = self.data.get_realtime_quote(symbol)
        except Exception:
            realtime = {}

        # 获取资金流向
        try:
            money_flow = self.data.get_money_flow(symbol)
        except Exception:
            money_flow = {}

        # 获取财务数据
        try:
            profit_sheet = self.data.get_profit_sheet_summary(symbol)
        except Exception:
            profit_sheet = {}

        try:
            cash_flow = self.data.get_cash_flow_summary(symbol)
        except Exception:
            cash_flow = {}

        try:
            dividend = self.data.get_dividend(symbol)
        except Exception:
            dividend = []

        try:
            profit_forecast = self.data.get_profit_forecast(symbol)
        except Exception:
            profit_forecast = {}

        try:
            major_holders = self.data.get_major_shareholders(symbol)
        except Exception:
            major_holders = []

        # 计算各项信号
        signals = self._generate_signals(df)

        # 估值辅助（从财务报表计算 PE/PB）
        current_price = realtime.get("price", df.iloc[-1]["close"])
        valuation = self.data.get_valuation(symbol, current_price)
        valuation = self._estimate_valuation(valuation)  # 估值判断

        # 构建报告
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        report = {
            "symbol": symbol,
            "name": realtime.get("name", symbol),
            "price": current_price,
            "pct_change": realtime.get("change_pct", 0),
            "volume": realtime.get("volume", latest["volume"]),
            "turnover": realtime.get("turnover", 0),
            "pe": valuation.get("pe", "-"),
            "pb": valuation.get("pb", "-"),
            "roe": valuation.get("roe"),
            "date": str(df.index[-1].date()) if hasattr(df.index[-1], "date") else str(df.index[-1])[:10],

            # 技术指标
            "ma": {
                "ma5": round(latest["ma5"], 2) if pd.notna(latest["ma5"]) else None,
                "ma10": round(latest["ma10"], 2) if pd.notna(latest["ma10"]) else None,
                "ma20": round(latest["ma20"], 2) if pd.notna(latest["ma20"]) else None,
                "ma60": round(latest["ma60"], 2) if pd.notna(latest["ma60"]) else None,
                "ma120": round(latest["ma120"], 2) if pd.notna(latest["ma120"]) else None,
                "ma250": round(latest["ma250"], 2) if pd.notna(latest["ma250"]) else None,
            },
            "trend": self._judge_trend(df),
            "macd": {
                "dif": round(latest["dif"], 4) if pd.notna(latest["dif"]) else None,
                "dea": round(latest["dea"], 4) if pd.notna(latest["dea"]) else None,
                "macd": round(latest["macd"], 4) if pd.notna(latest["macd"]) else None,
                "signal": signals["macd_signal"],
            },
            "kdj": {
                "k": round(latest["k"], 2) if pd.notna(latest["k"]) else None,
                "d": round(latest["d"], 2) if pd.notna(latest["d"]) else None,
                "j": round(latest["j"], 2) if pd.notna(latest["j"]) else None,
                "signal": signals["kdj_signal"],
            },
            "rsi": {
                "rsi6": round(latest["rsi6"], 2) if pd.notna(latest["rsi6"]) else None,
                "rsi12": round(latest["rsi12"], 2) if pd.notna(latest["rsi12"]) else None,
                "rsi24": round(latest["rsi24"], 2) if pd.notna(latest["rsi24"]) else None,
                "signal": signals["rsi_signal"],
            },
            "boll": {
                "upper": round(latest["boll_upper"], 2) if pd.notna(latest["boll_upper"]) else None,
                "mid": round(latest["boll_mid"], 2) if pd.notna(latest["boll_mid"]) else None,
                "lower": round(latest["boll_lower"], 2) if pd.notna(latest["boll_lower"]) else None,
                "position": round((latest["close"] - latest["boll_lower"]) /
                                  (latest["boll_upper"] - latest["boll_lower"]) * 100, 2)
                             if pd.notna(latest["boll_upper"]) and latest["boll_upper"] != latest["boll_lower"] else 50,
            },
            "signals": signals,
            "valuation": valuation,
            "money_flow": money_flow,
            "profit_sheet": profit_sheet,
            "cash_flow": cash_flow,
            "dividend": dividend,
            "profit_forecast": profit_forecast,
            "major_holders": major_holders,
            "buffett_view": self._buffett_reflection(symbol, realtime, signals, valuation),
        }

        return report

    def _judge_trend(self, df: pd.DataFrame) -> dict:
        """判断均线趋势多头/空头排列"""
        if len(df) < 60:
            return {"status": "数据不足", "score": 0}

        ma_cols = ["ma5", "ma10", "ma20", "ma60", "ma120"]
        latest = df.iloc[-1]
        prev20 = df.iloc[-20] if len(df) >= 20 else df.iloc[0]

        # 多头排列：短期 > 长期
        # 空头排列：短期 < 长期
        scores = []
        for i in range(len(ma_cols) - 1):
            if pd.notna(latest[ma_cols[i]]) and pd.notna(latest[ma_cols[i+1]]):
                if latest[ma_cols[i]] > latest[ma_cols[i+1]]:
                    scores.append(1)
                elif latest[ma_cols[i]] < latest[ma_cols[i+1]]:
                    scores.append(-1)
                else:
                    scores.append(0)

        avg_score = np.mean(scores) if scores else 0

        # 均线方向
        ma20_rising = latest["ma20"] > prev20["ma20"] if pd.notna(latest["ma20"]) and pd.notna(prev20["ma20"]) else None

        if avg_score > 0.3 and ma20_rising:
            status = "强势多头"
            score = 80
        elif avg_score > 0.1:
            status = "短线多头"
            score = 60
        elif avg_score < -0.3 and not ma20_rising:
            status = "弱势空头"
            score = 20
        elif avg_score < -0.1:
            status = "短线空头"
            score = 40
        else:
            status = "震荡整理"
            score = 50

        return {"status": status, "score": score, "ma20_rising": ma20_rising}

    def _generate_signals(self, df: pd.DataFrame) -> dict:
        """生成综合技术信号"""
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        signals = {}

        # MACD 信号
        if pd.notna(latest["dif"]) and pd.notna(latest["dea"]):
            if latest["dif"] > latest["dea"] and prev["dif"] <= prev["dea"]:
                signals["macd_signal"] = "MACD 金叉"
            elif latest["dif"] < latest["dea"] and prev["dif"] >= prev["dea"]:
                signals["macd_signal"] = "MACD 死叉"
            elif latest["macd"] > 0:
                signals["macd_signal"] = "MACD 红柱"
            else:
                signals["macd_signal"] = "MACD 绿柱"
        else:
            signals["macd_signal"] = "N/A"

        # KDJ 信号
        if pd.notna(latest["k"]) and pd.notna(latest["d"]):
            if latest["k"] > latest["d"] and prev["k"] <= prev["d"]:
                signals["kdj_signal"] = "KDJ 金叉"
            elif latest["k"] < latest["d"] and prev["k"] >= prev["d"]:
                signals["kdj_signal"] = "KDJ 死叉"
            elif latest["j"] > 80:
                signals["kdj_signal"] = "KDJ 超买"
            elif latest["j"] < 20:
                signals["kdj_signal"] = "KDJ 超卖"
            else:
                signals["kdj_signal"] = "KDJ 中性"
        else:
            signals["kdj_signal"] = "N/A"

        # RSI 信号
        if pd.notna(latest["rsi12"]):
            if latest["rsi12"] > 70:
                signals["rsi_signal"] = "RSI 超买"
            elif latest["rsi12"] < 30:
                signals["rsi_signal"] = "RSI 超卖"
            elif latest["rsi12"] > 50:
                signals["rsi_signal"] = "RSI 偏强"
            else:
                signals["rsi_signal"] = "RSI 偏弱"
        else:
            signals["rsi_signal"] = "N/A"

        # 布林带位置
        if pd.notna(latest["boll_upper"]) and latest["boll_upper"] != latest["boll_lower"]:
            boll_pos = (latest["close"] - latest["boll_lower"]) / (latest["boll_upper"] - latest["boll_lower"])
            if boll_pos > 0.9:
                signals["boll_signal"] = "触及上轨，注意压力"
            elif boll_pos < 0.1:
                signals["boll_signal"] = "触及下轨，注意支撑"
            else:
                signals["boll_signal"] = "布林带中轨运行"
        else:
            signals["boll_signal"] = "N/A"

        # 综合评分 (0-100)
        score = 50
        score += 10 if "金叉" in signals.get("macd_signal", "") else -10 if "死叉" in signals.get("macd_signal", "") else 0
        score += 10 if "金叉" in signals.get("kdj_signal", "") else -10 if "死叉" in signals.get("kdj_signal", "") else 0
        score += 5 if signals.get("rsi_signal") in ["RSI 偏强", "RSI 超买"] else -5 if signals.get("rsi_signal") in ["RSI 偏弱", "RSI 超卖"] else 0

        signals["composite_score"] = min(100, max(0, score))

        return signals

    def _estimate_valuation(self, valuation: dict) -> dict:
        """基于财务报表数据的估值判断"""
        val = {}
        pe = valuation.get("pe")
        pb = valuation.get("pb")
        roe = valuation.get("roe")

        if pe is not None and pe > 0:
            if pe < 15:
                val["pe_judge"] = "偏低"
            elif pe < 30:
                val["pe_judge"] = "合理"
            elif pe < 50:
                val["pe_judge"] = "偏高"
            else:
                val["pe_judge"] = "泡沫区"
            val["pe_value"] = pe
        else:
            val["pe_judge"] = "无数据"
            val["pe_value"] = None

        if pb is not None and pb > 0:
            if pb < 2:
                val["pb_judge"] = "偏低"
            elif pb < 5:
                val["pb_judge"] = "合理"
            elif pb < 10:
                val["pb_judge"] = "偏高"
            else:
                val["pb_judge"] = "泡沫区"
            val["pb_value"] = pb
        else:
            val["pb_judge"] = "无数据"
            val["pb_value"] = None

        if roe is not None and roe > 0:
            val["roe"] = roe
            if roe > 20:
                val["roe_judge"] = "优秀"
            elif roe > 10:
                val["roe_judge"] = "良好"
            elif roe > 0:
                val["roe_judge"] = "一般"
            else:
                val["roe_judge"] = "亏损"
        else:
            val["roe_judge"] = "无数据"
            val["roe"] = None

        return val

    def _buffett_reflection(self, symbol: str, realtime: dict, signals: dict, valuation: dict) -> dict:
        """
        巴菲特视角反思
        从护城河、安全边际、管理层、竞争格局角度给出文字提醒
        """
        name = realtime.get("name", symbol)
        pe_judge = valuation.get("pe_judge", "无数据")
        composite = signals.get("composite_score", 50)

        reflections = []

        # 安全边际
        if pe_judge == "偏低":
            reflections.append(f"【安全边际】{name}当前PE估值偏低，提供了较好的安全边际")
        elif pe_judge == "偏高":
            reflections.append(f"【风险提示】{name}当前估值偏高，需等待更好的买入时机")
        elif pe_judge == "泡沫区":
            reflections.append(f"【高度警惕】{name}处于泡沫区，安全边际严重不足")

        # 技术面
        if composite >= 70:
            reflections.append(f"【技术强势】综合评分{composite}，短期动能充足，但需注意追高风险")
        elif composite <= 30:
            reflections.append(f"【技术弱势】综合评分{composite}，趋势向下，不宜盲目抄底")

        # 仓位提醒
        reflections.append("【仓位原则】永远不要满仓一只股票，分散投资是铁律")

        # 护城河提醒
        reflections.append("【护城河自检】这只股票的护城河是什么？品牌？垄断？网络效应？")

        return {
            "reflection_points": reflections,
            "verdict": "可关注" if pe_judge in ["偏低", "合理"] and composite >= 50 else "谨慎观望"
        }

    def format_markdown(self, report: dict) -> str:
        """格式化输出为 Markdown 报告"""
        symbol = report["symbol"]
        name = report["name"]
        price = report["price"]
        pct = report["pct_change"]
        trend = report["trend"]
        signals = report["signals"]
        ma = report["ma"]
        macd = report["macd"]
        kdj = report["kdj"]
        rsi = report["rsi"]
        boll = report["boll"]
        valuation = report["valuation"]
        profit_sheet = report.get("profit_sheet", {})
        cash_flow = report.get("cash_flow", {})
        dividend = report.get("dividend", [])
        profit_forecast = report.get("profit_forecast", {})
        major_holders = report.get("major_holders", [])
        buffett = report["buffett_view"]

        change_emoji = "🔴" if pct > 0 else "🔵"
        trend_emoji = "🟢" if "多头" in trend["status"] else "🔴" if "空头" in trend["status"] else "🟡"

        lines = [
            f"# {name}（{symbol}）技术分析报告",
            f"",
            f"**日期**: {report['date']}  |  **现价**: {price}  |  **涨跌幅**: {change_emoji} {pct}%",
            f"",
            f"---",
            f"",
            f"## 📊 核心数据",
            f"",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 趋势 | {trend_emoji} {trend['status']} (评分: {trend['score']}) |",
            f"| 综合评分 | {signals['composite_score']}/100 |",
            f"| MA20方向 | {'上升↗' if trend.get('ma20_rising') else '下降↘'} |",
            f"| 市盈率PE | {report['pe']} ({valuation.get('pe_judge')}) |",
            f"| 市净率PB | {report['pb']} ({valuation.get('pb_judge')}) |",
            f"| 净资产收益率ROE | {report['roe']}% ({valuation.get('roe_judge')}) |",
            f"| 换手率 | {report['turnover']}% |",
            f"",
            f"## 📈 均线系统",
            f"",
            f"| 均线 | 数值 |",
            f"|------|------|",
            f"| MA5 | {ma['ma5']} |",
            f"| MA10 | {ma['ma10']} |",
            f"| MA20 | {ma['ma20']} |",
            f"| MA60 | {ma['ma60']} |",
            f"| MA120 | {ma['ma120']} |",
            f"| MA250 | {ma['ma250']} |",
            f"",
            f"## 📉 MACD",
            f"",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| DIF | {macd['dif']} |",
            f"| DEA | {macd['dea']} |",
            f"| MACD柱 | {macd['macd']} |",
            f"| 信号 | {macd['signal']} |",
            f"",
            f"## 📊 KDJ",
            f"",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| K | {kdj['k']} |",
            f"| D | {kdj['d']} |",
            f"| J | {kdj['j']} |",
            f"| 信号 | {kdj['signal']} |",
            f"",
            f"## 📊 RSI",
            f"",
            f"| 周期 | 数值 |",
            f"|------|------|",
            f"| RSI6 | {rsi['rsi6']} |",
            f"| RSI12 | {rsi['rsi12']} |",
            f"| RSI24 | {rsi['rsi24']} |",
            f"| 信号 | {rsi['signal']} |",
            f"",
            f"## 📊 布林带",
            f"",
            f"| 轨道 | 数值 |",
            f"|------|------|",
            f"| 上轨 | {boll['upper']} |",
            f"| 中轨 | {boll['mid']} |",
            f"| 下轨 | {boll['lower']} |",
            f"| 当前位置 | {boll['position']}% (0%=下轨, 100%=上轨) |",
            f"| 信号 | {signals.get('boll_signal', 'N/A')} |",
            f"",

            # 利润表摘要
            f"## 💰 利润表摘要",
            f"",
        ]

        if profit_sheet and "error" not in profit_sheet:
            lines.extend([
                f"| 指标 | 数值 |",
                f"|------|------|",
                f"| 报告期 | {profit_sheet.get('report_date', 'N/A')} |",
                f"| 营业收入 | {self._format_yuan(profit_sheet.get('revenue', 0))} |",
                f"| 净利润 | {self._format_yuan(profit_sheet.get('net_profit', 0))} |",
                f"| 毛利率 | {profit_sheet.get('gross_margin', 0)}% |",
                f"| 净利率 | {profit_sheet.get('net_margin', 0)}% |",
                f"| 基本EPS | {profit_sheet.get('basic_eps', 0)} |",
                f"",
            ])
        else:
            lines.append(f"_利润表数据暂不可用_")
            lines.append(f"")

        # 现金流量表摘要
        lines.append(f"## 💵 现金流量表")
        lines.append(f"")
        if cash_flow and "error" not in cash_flow:
            lines.extend([
                f"| 指标 | 数值 |",
                f"|------|------|",
                f"| 报告期 | {cash_flow.get('report_date', 'N/A')} |",
                f"| 经营现金流 | {self._format_yuan(cash_flow.get('operating_cf', 0))} |",
                f"| 投资现金流 | {self._format_yuan(cash_flow.get('investing_cf', 0))} |",
                f"| 筹资现金流 | {self._format_yuan(cash_flow.get('financing_cf', 0))} |",
                f"| 自由现金流 | {self._format_yuan(cash_flow.get('free_cf', 0))} |",
                f"| 期末现金 | {self._format_yuan(cash_flow.get('cash_end', 0))} |",
                f"",
            ])
        else:
            lines.append(f"_现金流量表数据暂不可用_")
            lines.append(f"")

        # 分红历史
        lines.append(f"## 📋 历史分红（近5次）")
        lines.append(f"")
        if dividend and all("error" not in d for d in dividend):
            lines.extend([
                f"| 公告日期 | 分红方案 | 股权登记日 | 除权日 | 派息日 |",
                f"|------|------|------|------|------|",
            ])
            for d in dividend[:5]:
                div_str = f"每股{d.get('dividend_per_share', 0)}元" if d.get('dividend_per_share', 0) > 0 else "—"
                lines.append(f"| {d.get('date', 'N/A')} | {div_str} | {d.get('record_date', 'N/A')} | {d.get('ex_date', 'N/A')} | {d.get('pay_date', 'N/A')} |")
            lines.append(f"")
        else:
            lines.append(f"_分红数据暂不可用_")
            lines.append(f"")

        # 盈利预测
        lines.append(f"## 🔮 券商盈利预测")
        lines.append(f"")
        forecasts = profit_forecast.get("forecasts", [])
        if forecasts:
            lines.extend([
                f"| 年度 | 预测EPS均值 | 最小值 | 最大值 | 预测机构数 |",
                f"|------|------|------|------|------|",
            ])
            for f in forecasts[:3]:
                lines.append(f"| {f.get('year', 'N/A')} | {f.get('avg_eps', 0)} | {f.get('min_eps', 0)} | {f.get('max_eps', 0)} | {f.get('num_firms', 0)}家 |")
            lines.append(f"")
        else:
            lines.append(f"_盈利预测暂不可用_")
            lines.append(f"")

        # 前十大股东
        lines.append(f"## 🏛️ 前十大股东")
        lines.append(f"")
        if major_holders and all("error" not in h for h in major_holders):
            lines.extend([
                f"| 股东名称 | 持股比例 | 股本性质 | 截至日期 |",
                f"|------|------|------|------|",
            ])
            for h in major_holders[:10]:
                lines.append(f"| {h.get('name', 'N/A')} | {h.get('pct', 0)}% | {h.get('nature', 'N/A')} | {h.get('截止日期', 'N/A')} |")
            lines.append(f"")
        else:
            lines.append(f"_股东数据暂不可用_")
            lines.append(f"")

        lines.append(f"## 🧠 巴菲特视角")
        lines.append(f"")

        for point in buffett["reflection_points"]:
            lines.append(f"- {point}")

        lines.append(f"")
        lines.append(f"**结论**: {buffett['verdict']}")
        lines.append(f"")
        lines.append(f"---")
        lines.append(f"")
        lines.append(f"*本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。*")

        return "\n".join(lines)

    @staticmethod
    def _format_yuan(val: float) -> str:
        """格式化金额为亿/万单位"""
        if val is None:
            return "N/A"
        abs_val = abs(val)
        if abs_val >= 1e8:
            return f"{val/1e8:.2f}亿"
        elif abs_val >= 1e4:
            return f"{val/1e4:.2f}万"
        else:
            return f"{val:.2f}"

    def chart(self, symbol: str, days: int = 120,
              adjust: Literal["qfq", "hfq", ""] = "qfq",
              save_path: str = None,
              style: str = "dark") -> None:
        """
        弹出 K线图表（含 MACD/KDJ/RSI/成交量）

        Args:
            symbol: 股票代码，如 "600519"
            days: 显示天数，默认120天
            adjust: 复权类型 qfq=前复权 hfq=后复权 ""=不复权
            save_path: 可选，保存路径（如 "600519.png"）
            style: "dark" 深色主题，"light" 浅色主题
        """
        df = self.data.get_daily(symbol, adjust=adjust)
        df = df.tail(days)
        ind_df = self.ind.all_in_one(df)
        df = pd.concat([df, ind_df], axis=1)

        charts = MoatXCharts(df, symbol)
        charts.plot(save_path=save_path, style=style)

    def screen(
        self,
        # 基本面
        pe_range: tuple = (0, 50),
        pb_range: tuple = (0, 10),
        cap_min: float = None,
        cap_max: float = None,
        turnover_min: float = None,
        # 技术/资金
        pct_change_min: float = None,
        volume_ratio_min: float = None,
        money_in_min: float = None,
        # 排序
        sort_by: str = "pct_change",
        ascending: bool = False,
        limit: int = 30
    ) -> pd.DataFrame:
        """
        快速选股：在全市场实时行情上过滤

        Returns:
            DataFrame: 筛选结果
        """
        screener = MoatXScreener()
        result = screener.scan_all(
            pe_range=pe_range,
            pb_range=pb_range,
            cap_min=cap_min,
            cap_max=cap_max,
            turnover_min=turnover_min,
            pct_change_min=pct_change_min,
            volume_ratio_min=volume_ratio_min,
            sort_by=sort_by,
            ascending=ascending,
            limit=limit
        )
        return result

    def format_screening_markdown(self, df: pd.DataFrame) -> str:
        """格式化选股结果为 Markdown"""
        if df.empty:
            return "**暂无符合条件的结果**"

        lines = [
            f"# MoatX 选股结果",
            f"",
            f"| 代码 | 名称 | 现价 | 涨跌幅 | PE | PB | 换手率 | 综合评分 |",
            f"|------|------|------|--------|----|----|--------|--------|",
        ]

        for _, row in df.iterrows():
            lines.append(
                f"| {row.get('code', '')} | {row.get('name', '')} | "
                f"{row.get('price', 0):.2f} | {row.get('pct_change', 0):.2f}% | "
                f"{row.get('pe', '-')} | {row.get('pb', '-')} | "
                f"{row.get('turnover', 0):.2f}% | {row.get('composite', 0):.1f} |"
            )

        lines.append(f"")
        lines.append(f"_共 {len(df)} 只符合条件_")
        lines.append(f"")
        lines.append(f"*评分维度：趋势25% + 估值25% + 资金25% + 动量25%*")

        return "\n".join(lines)
