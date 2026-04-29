"""
analyzer.py - MoatX 核心分析引擎
把数据获取 + 指标计算 + 信号判断 + 报告生成 整合在一起
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Literal, Any, TypedDict, NotRequired

import pandas as pd
import numpy as np

_logger = logging.getLogger("moatx.analyzer")

from .stock_data import StockData
from .indicators import IndicatorEngine
from .charts import MoatXCharts
from .screener import MoatXScreener
from .calendar import is_trading_day


# ------------------------------------------------------------------
# TypedDict definitions for analyzer report
# ------------------------------------------------------------------

class MAData(TypedDict):
    ma5: Optional[float]
    ma10: Optional[float]
    ma20: Optional[float]
    ma60: Optional[float]
    ma120: Optional[float]
    ma250: Optional[float]


class MACDData(TypedDict):
    dif: Optional[float]
    dea: Optional[float]
    macd: Optional[float]
    signal: str


class KDJData(TypedDict):
    k: Optional[float]
    d: Optional[float]
    j: Optional[float]
    signal: str


class RSIData(TypedDict):
    rsi6: Optional[float]
    rsi12: Optional[float]
    rsi24: Optional[float]
    signal: str


class BOLLData(TypedDict):
    upper: Optional[float]
    mid: Optional[float]
    lower: Optional[float]
    position: float  # 0.0~100.0


class TrendData(TypedDict):
    status: str
    score: float
    ma20_rising: Optional[bool]


class SignalsData(TypedDict):
    macd_signal: str
    kdj_signal: str
    rsi_signal: str
    boll_signal: str
    composite_score: int


class ValuationData(TypedDict):
    pe_judge: str
    pb_judge: str
    roe_judge: str
    pe_value: Optional[float]
    pb_value: Optional[float]
    roe: Optional[float]


class FinancialRiskData(TypedDict):
    risk_score: int
    risk_level: str
    is_buyable: bool
    red_flags: NotRequired[list[str]]
    warnings: NotRequired[list[str]]


class BuffettViewData(TypedDict):
    reflection_points: list[str]
    verdict: str


class ProfitSheetData(TypedDict):
    report_date: str
    revenue: NotRequired[float]
    net_profit: NotRequired[float]
    gross_margin: NotRequired[float]
    net_margin: NotRequired[float]
    basic_eps: NotRequired[float]


class CashFlowData(TypedDict):
    report_date: str
    operating_cf: NotRequired[float]
    investing_cf: NotRequired[float]
    financing_cf: NotRequired[float]
    free_cf: NotRequired[float]
    cash_end: NotRequired[float]


class DividendRecord(TypedDict):
    date: str
    dividend_per_share: float
    record_date: str
    ex_date: str
    pay_date: str


class ForecastRecord(TypedDict):
    year: int
    avg_eps: float
    min_eps: float
    max_eps: float
    num_firms: int


class ProfitForecastData(TypedDict):
    forecasts: NotRequired[list[ForecastRecord]]


class MajorHolderRecord(TypedDict):
    name: str
    pct: float
    nature: str
    截止日期: str


class MoneyFlowData(TypedDict):
    inflow: NotRequired[float]
    outflow: NotRequired[float]


class AnalyzerReport(TypedDict):
    symbol: str
    name: str
    price: float
    pct_change: float
    volume: float
    turnover: float
    pe: Any  # may be '-' or float
    pb: Any  # may be '-' or float
    roe: Any  # may be None or float
    date: str
    ma: MAData
    trend: TrendData
    macd: MACDData
    kdj: KDJData
    rsi: RSIData
    boll: BOLLData
    signals: SignalsData
    valuation: ValuationData
    money_flow: NotRequired[MoneyFlowData]
    profit_sheet: NotRequired[ProfitSheetData]
    cash_flow: NotRequired[CashFlowData]
    dividend: NotRequired[list[DividendRecord]]
    profit_forecast: NotRequired[ProfitForecastData]
    major_holders: NotRequired[list[MajorHolderRecord]]
    financial_risk: FinancialRiskData
    buffett_view: BuffettViewData


class MoatXAnalyzer:
    """
    MoatX A股分析器
    使用方法:
        analyzer = MoatXAnalyzer()
        report = analyzer.analyze("600519")
        print(report)
    """

    def __init__(self) -> None:
        self.data = StockData()
        self.ind = IndicatorEngine()

    def analyze(
        self,
        symbol: str,
        days: int | None = None,
        adjust: Literal["qfq", "hfq", ""] = "qfq"
    ) -> AnalyzerReport:
        """
        综合分析一只股票

        Returns:
            dict: 包含实时行情、技术指标、信号判断、风险评估
        """
        # 获取数据
        df = self.data.get_daily(symbol, adjust=adjust)
        if days is not None:
            df = df.tail(days)

        # 计算指标
        ind_df = self.ind.all_in_one(df)
        df = pd.concat([df, ind_df], axis=1)

        # 获取实时行情
        try:
            realtime = self.data.get_realtime_quote(symbol)
        except Exception as e:
            _logger.warning("获取实时行情失败（已降级）: %s", e)
            realtime = {}

        # 并行获取 6 个财务数据（串行约 ~10s+，并行约 ~3s）
        _fin_results: dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {
                ex.submit(self.data.get_money_flow, symbol): "money_flow",
                ex.submit(self.data.get_profit_sheet_summary, symbol): "profit_sheet",
                ex.submit(self.data.get_cash_flow_summary, symbol): "cash_flow",
                ex.submit(self.data.get_dividend, symbol): "dividend",
                ex.submit(self.data.get_profit_forecast, symbol): "profit_forecast",
                ex.submit(self.data.get_major_shareholders, symbol): "major_holders",
            }
            for fut in as_completed(futures, timeout=15):
                key = futures[fut]
                try:
                    val = fut.result()
                    _fin_results[key] = val if val else {}
                except Exception as e:
                    _logger.warning("获取 %s 失败（已降级）: %s", key, e)
                    _fin_results[key] = [] if key in ("dividend", "major_holders") else {}

        money_flow = _fin_results.get("money_flow", {})
        profit_sheet = _fin_results.get("profit_sheet", {})
        cash_flow = _fin_results.get("cash_flow", {})
        dividend = _fin_results.get("dividend", [])
        profit_forecast = _fin_results.get("profit_forecast", {})
        major_holders = _fin_results.get("major_holders", [])

        # 计算各项信号
        signals = self._generate_signals(df)

        # 财务风险检测（独立查询，快速失败）
        try:
            financial_risk = self.data.check_financial_risk(symbol)
        except Exception as e:
            _logger.warning("财务风险检测失败（已降级）: %s", e)
            financial_risk = {"risk_score": 0, "risk_level": "基本无风险", "is_buyable": True}

        # 估值辅助（从财务报表计算 PE/PB）
        current_price = realtime.get("price", df.iloc[-1]["close"])
        valuation = self.data.get_valuation(symbol, current_price)
        valuation = self._estimate_valuation(valuation)  # 估值判断

        # 构建报告
        latest = df.iloc[-1]

        report = {
            "symbol": symbol,
            "name": realtime.get("name", symbol),
            "price": current_price,
            "pct_change": realtime.get("change_pct", 0),
            "_notice": "" if (current_price > 0 or is_trading_day()) else "非交易时段，以下数据来自上一交易日",
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
            "financial_risk": financial_risk,
            "buffett_view": self._buffett_reflection(symbol, realtime, signals, valuation, financial_risk),
        }

        return report

    def _judge_trend(self, df: pd.DataFrame) -> TrendData:
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

    def _generate_signals(self, df: pd.DataFrame) -> SignalsData:
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

    def _estimate_valuation(self, valuation: dict[str, Any]) -> ValuationData:
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

    def _buffett_reflection(self, symbol: str, realtime: dict[str, Any], signals: SignalsData,
                              valuation: ValuationData, financial_risk: FinancialRiskData | None = None) -> BuffettViewData:
        """
        巴菲特视角反思
        从护城河、安全边际、管理层、竞争格局角度给出文字提醒
        """
        name = realtime.get("name", symbol)
        pe_judge = valuation.get("pe_judge", "无数据")
        composite = signals.get("composite_score", 50)
        frisk = financial_risk or {}

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

        # 财务风险（新增）
        if not frisk.get("is_buyable", True):
            reflections.append(f"【财务风险】风险评分{frisk.get('risk_score',0)}分（{frisk.get('risk_level','未知')}），存在重大风险信号，强烈不建议买入")
        elif frisk.get("risk_score", 0) >= 30:
            reflections.append(f"【风险提示】财务风险评分{frisk.get('risk_score',0)}分（{frisk.get('risk_level','未知')}），需谨慎评估")

        # 仓位提醒
        reflections.append("【仓位原则】永远不要满仓一只股票，分散投资是铁律")

        # 护城河提醒
        reflections.append("【护城河自检】这只股票的护城河是什么？品牌？垄断？网络效应？")

        verdict = "谨慎观望"
        if frisk.get("is_buyable", True) and pe_judge in ["偏低", "合理"] and composite >= 50:
            verdict = "可关注"
        elif not frisk.get("is_buyable", True):
            verdict = "风险过高"

        return {
            "reflection_points": reflections,
            "verdict": verdict
        }

    def format_markdown(self, report: AnalyzerReport) -> str:
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
        frisk = report.get("financial_risk", {})

        change_emoji = "🔴" if pct > 0 else "🔵"
        trend_emoji = "🟢" if "多头" in trend["status"] else "🔴" if "空头" in trend["status"] else "🟡"

        lines = [
            f"# {name}（{symbol}）技术分析报告",
            "",
            f"**日期**: {report['date']}  |  **现价**: {price}  |  **涨跌幅**: {change_emoji} {pct}%",
            "",
            "---",
            "",
            "## 📊 核心数据",
            "",
            "| 指标 | 数值 |",
            "|------|------|",
            f"| 趋势 | {trend_emoji} {trend['status']} (评分: {trend['score']}) |",
            f"| 综合评分 | {signals['composite_score']}/100 |",
            f"| MA20方向 | {'上升↗' if trend.get('ma20_rising') else '下降↘'} |",
            f"| 市盈率PE | {report['pe']} ({valuation.get('pe_judge')}) |",
            f"| 市净率PB | {report['pb']} ({valuation.get('pb_judge')}) |",
            f"| 净资产收益率ROE | {report['roe']}% ({valuation.get('roe_judge')}) |",
            f"| 换手率 | {report['turnover']}% |",
            "",
            "## 📈 均线系统",
            "",
            "| 均线 | 数值 |",
            "|------|------|",
            f"| MA5 | {ma['ma5']} |",
            f"| MA10 | {ma['ma10']} |",
            f"| MA20 | {ma['ma20']} |",
            f"| MA60 | {ma['ma60']} |",
            f"| MA120 | {ma['ma120']} |",
            f"| MA250 | {ma['ma250']} |",
            "",
            "## 📉 MACD",
            "",
            "| 指标 | 数值 |",
            "|------|------|",
            f"| DIF | {macd['dif']} |",
            f"| DEA | {macd['dea']} |",
            f"| MACD柱 | {macd['macd']} |",
            f"| 信号 | {macd['signal']} |",
            "",
            "## 📊 KDJ",
            "",
            "| 指标 | 数值 |",
            "|------|------|",
            f"| K | {kdj['k']} |",
            f"| D | {kdj['d']} |",
            f"| J | {kdj['j']} |",
            f"| 信号 | {kdj['signal']} |",
            "",
            "## 📊 RSI",
            "",
            "| 周期 | 数值 |",
            "|------|------|",
            f"| RSI6 | {rsi['rsi6']} |",
            f"| RSI12 | {rsi['rsi12']} |",
            f"| RSI24 | {rsi['rsi24']} |",
            f"| 信号 | {rsi['signal']} |",
            "",
            "## 📊 布林带",
            "",
            "| 轨道 | 数值 |",
            "|------|------|",
            f"| 上轨 | {boll['upper']} |",
            f"| 中轨 | {boll['mid']} |",
            f"| 下轨 | {boll['lower']} |",
            f"| 当前位置 | {boll['position']}% (0%=下轨, 100%=上轨) |",
            f"| 信号 | {signals.get('boll_signal', 'N/A')} |",
            "",

            # 利润表摘要
            "## 💰 利润表摘要",
            "",
        ]

        if profit_sheet and "error" not in profit_sheet:
            lines.extend([
                "| 指标 | 数值 |",
                "|------|------|",
                f"| 报告期 | {profit_sheet.get('report_date', 'N/A')} |",
                f"| 营业收入 | {self._format_yuan(profit_sheet.get('revenue', 0))} |",
                f"| 净利润 | {self._format_yuan(profit_sheet.get('net_profit', 0))} |",
                f"| 毛利率 | {profit_sheet.get('gross_margin', 0)}% |",
                f"| 净利率 | {profit_sheet.get('net_margin', 0)}% |",
                f"| 基本EPS | {profit_sheet.get('basic_eps', 0)} |",
                "",
            ])
        else:
            lines.append("_利润表数据暂不可用_")
            lines.append("")

        # 现金流量表摘要
        lines.append("## 💵 现金流量表")
        lines.append("")
        if cash_flow and "error" not in cash_flow:
            lines.extend([
                "| 指标 | 数值 |",
                "|------|------|",
                f"| 报告期 | {cash_flow.get('report_date', 'N/A')} |",
                f"| 经营现金流 | {self._format_yuan(cash_flow.get('operating_cf', 0))} |",
                f"| 投资现金流 | {self._format_yuan(cash_flow.get('investing_cf', 0))} |",
                f"| 筹资现金流 | {self._format_yuan(cash_flow.get('financing_cf', 0))} |",
                f"| 自由现金流 | {self._format_yuan(cash_flow.get('free_cf', 0))} |",
                f"| 期末现金 | {self._format_yuan(cash_flow.get('cash_end', 0))} |",
                "",
            ])
        else:
            lines.append("_现金流量表数据暂不可用_")
            lines.append("")

        # 分红历史
        lines.append("## 📋 历史分红（近5次）")
        lines.append("")
        if dividend and all("error" not in d for d in dividend):
            lines.extend([
                "| 公告日期 | 分红方案 | 股权登记日 | 除权日 | 派息日 |",
                "|------|------|------|------|------|",
            ])
            for d in dividend[:5]:
                div_str = f"每股{d.get('dividend_per_share', 0)}元" if d.get('dividend_per_share', 0) > 0 else "—"
                lines.append(f"| {d.get('date', 'N/A')} | {div_str} | {d.get('record_date', 'N/A')} | {d.get('ex_date', 'N/A')} | {d.get('pay_date', 'N/A')} |")
            lines.append("")
        else:
            lines.append("_分红数据暂不可用_")
            lines.append("")

        # 盈利预测
        lines.append("## 🔮 券商盈利预测")
        lines.append("")
        forecasts = profit_forecast.get("forecasts", [])
        if forecasts:
            lines.extend([
                "| 年度 | 预测EPS均值 | 最小值 | 最大值 | 预测机构数 |",
                "|------|------|------|------|------|",
            ])
            for f in forecasts[:3]:
                lines.append(f"| {f.get('year', 'N/A')} | {f.get('avg_eps', 0)} | {f.get('min_eps', 0)} | {f.get('max_eps', 0)} | {f.get('num_firms', 0)}家 |")
            lines.append("")
        else:
            lines.append("_盈利预测暂不可用_")
            lines.append("")

        # 前十大股东
        lines.append("## 🏛️ 前十大股东")
        lines.append("")
        if major_holders and all("error" not in h for h in major_holders):
            lines.extend([
                "| 股东名称 | 持股比例 | 股本性质 | 截至日期 |",
                "|------|------|------|------|",
            ])
            for h in major_holders[:10]:
                lines.append(f"| {h.get('name', 'N/A')} | {h.get('pct', 0)}% | {h.get('nature', 'N/A')} | {h.get('截止日期', 'N/A')} |")
            lines.append("")
        else:
            lines.append("_股东数据暂不可用_")
            lines.append("")

        lines.append("## 🛡️ 财务风险检测")
        lines.append("")
        risk_score = frisk.get("risk_score", 0)
        risk_level = frisk.get("risk_level", "未知")
        is_buyable = frisk.get("is_buyable", True)
        risk_emoji = "🚨" if risk_score >= 50 else "⚠️" if risk_score >= 30 else "✅"
        lines.append(f"- 风险评分: **{risk_score}** 分（{risk_level}）{risk_emoji}")
        lines.append(f"- 是否建议买入: **{'✅ 可买入' if is_buyable else '❌ 不建议买入'}**")
        for flag in frisk.get("red_flags", []):
            lines.append(f"- ⚠️ {flag}")
        for w in frisk.get("warnings", []):
            lines.append(f"- ⚡ {w}")
        lines.append("")

        lines.append("## 🧠 巴菲特视角")
        lines.append("")

        for point in buffett["reflection_points"]:
            lines.append(f"- {point}")

        lines.append("")
        lines.append(f"**结论**: {buffett['verdict']}")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("*本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。*")

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
              save_path: str | None = None,
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
        pe_range: tuple[float, float] = (0, 50),
        pb_range: tuple[float, float] = (0, 10),
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
            "# MoatX 选股结果",
            "",
            "| 代码 | 名称 | 现价 | 涨跌幅 | PE | PB | 换手率 | 综合评分 |",
            "|------|------|------|--------|----|----|--------|--------|",
        ]

        for _, row in df.iterrows():
            lines.append(
                f"| {row.get('code', '')} | {row.get('name', '')} | "
                f"{row.get('price', 0):.2f} | {row.get('pct_change', 0):.2f}% | "
                f"{row.get('pe', '-')} | {row.get('pb', '-')} | "
                f"{row.get('turnover', 0):.2f}% | {row.get('composite', 0):.1f} |"
            )

        lines.append("")
        lines.append(f"_共 {len(df)} 只符合条件_")
        lines.append("")
        lines.append("*评分维度：趋势25% + 估值25% + 资金25% + 动量25%*")

        return "\n".join(lines)
