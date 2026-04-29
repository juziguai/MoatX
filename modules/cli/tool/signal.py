"""
modules/cli/tool/signal.py - 交易信号
"""

STRATEGY_MAP = {
    "ma": "modules.strategy.library:MovingAverageCross",
    "kdj": "modules.strategy.library:KDJStrategy",
}


def cmd_signal(args):
    from modules.db import DatabaseManager
    from modules.config import cfg as _cfg
    from modules.signal.engine import SignalEngine
    from modules.signal.journal import SignalJournal
    from modules.stock_data import StockData
    from modules.portfolio import Portfolio

    db = DatabaseManager(_cfg().data.warehouse_path)
    journal = SignalJournal(db)

    if args.action == "list":
        df = journal.recent(limit=args.limit)
        if df.empty:
            print("No signal records")
            return
        cols = ["id", "symbol", "signal_type", "price", "confidence", "reason", "created_at"]
        print(df[[c for c in cols if c in df.columns]].to_string(index=False))
        return

    if args.action == "run":
        engine = SignalEngine(db)
        try:
            pf = Portfolio()
            holdings = pf.list_holdings()
            symbols = holdings["code"].tolist() if not holdings.empty else []
        except Exception:
            symbols = []
        if args.symbol:
            symbols = [args.symbol] if args.symbol not in symbols else [args.symbol]

        if not symbols:
            try:
                sd = StockData()
                spot = sd.get_spot()
                symbols = spot["code"].head(20).tolist() if not spot.empty else ["600519"]
            except Exception:
                symbols = ["600519"]

        # Load strategy
        strategy_cls_path = STRATEGY_MAP.get(args.strategy, STRATEGY_MAP["ma"])
        mod_path, cls_name = strategy_cls_path.rsplit(":", 1)
        mod = __import__(mod_path, fromlist=[cls_name])
        strategy_cls = getattr(mod, cls_name)
        strategy = strategy_cls()

        # Apply params from JSON if provided
        params_file = getattr(args, "params_file", None)
        if params_file or args.strategy:
            # Try to load params from JSON (default: data/strategy_params.json)
            strategy_name = strategy_cls.__name__
            loaded_params = SignalEngine.load_params(strategy_name, params_file)
            if loaded_params:
                strategy.set_params(**loaded_params)
                print(f"[{strategy_name}] params loaded from {params_file or 'data/strategy_params.json'}: {loaded_params}")
            elif params_file:
                print(f"[{strategy_name}] params file not found: {params_file}")
            else:
                print(f"[{strategy_name}] no params file found, using defaults")

        signals = engine.evaluate_all(symbols, strategy)
        if not signals:
            print("No signals generated")
            return
        for sig in signals:
            journal.record(sig)
            print(f"  [{sig.signal_type.upper()}] {sig.symbol} @ {sig.price:.2f} "
                  f"(confidence: {sig.confidence:.0f}%) - {sig.reason}")
        print(f"Total: {len(signals)} signal(s)")
        return

    if args.action == "clear":
        print("Batch clear not supported. Please operate warehouse.db directly.")
