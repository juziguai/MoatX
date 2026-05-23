"""CLI for single-stock decision reports."""

from __future__ import annotations

import json
import logging


def cmd_stock_report(args) -> None:
    if not getattr(args, "verbose", False):
        logging.disable(logging.WARNING)

    from modules.stock_decision_report import StockDecisionReporter

    payload = StockDecisionReporter().build(args.symbol)
    if args.as_json:
        print(json.dumps({k: v for k, v in payload.items() if k != "markdown"}, ensure_ascii=False, indent=2))
    else:
        print(payload["markdown"])
