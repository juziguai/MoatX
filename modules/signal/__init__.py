"""交易信号集成 — 信号生成、日志记录、模拟交易。"""

from .engine import SignalEngine as SignalEngine, Signal as Signal, SignalType as SignalType
from .journal import SignalJournal as SignalJournal
from .paper_trader import PaperTrader as PaperTrader
