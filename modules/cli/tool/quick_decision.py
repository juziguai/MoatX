"""CLI wrapper for fast intraday buy/no-buy decisions."""

from __future__ import annotations

import json


def cmd_quick_decision(args) -> None:
    from modules.quick_decision import (
        build_quick_decision,
        evaluate_quick_decisions,
        load_watchlist_symbols,
        print_quick_decision,
        print_quick_decision_evaluation,
        print_quick_decision_review,
        print_quick_decision_summary,
        review_quick_decisions,
        save_quick_decision,
        summarize_quick_decision_evaluations,
    )

    mode = args.symbols[0] if args.symbols and args.symbols[0] in {"review", "evaluate", "summary", "dashboard"} else ""
    review_mode = args.review or mode == "review"
    evaluate_mode = mode == "evaluate"
    summary_mode = mode in {"summary", "dashboard"}
    review_symbols = args.symbols[1:] if mode else args.symbols
    if summary_mode:
        payload = summarize_quick_decision_evaluations(
            horizon_days=args.horizon,
            limit=args.limit,
            min_samples=args.min_samples,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_summary(payload)
        return
    if evaluate_mode or args.save_evaluation:
        payload = evaluate_quick_decisions(
            review_symbols,
            horizons=_parse_horizons(args.horizons),
            limit=args.limit,
            action=args.action,
            save=True,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_evaluation(payload)
        return
    if review_mode:
        payload = review_quick_decisions(
            review_symbols,
            limit=args.limit,
            horizon_days=args.horizon,
            action=args.action,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_review(payload)
        return
    if not args.symbols:
        if not args.watchlist:
            raise SystemExit("请提供股票代码，或使用 --watchlist / --review")

    symbols = list(args.symbols)
    if args.watchlist:
        symbols = [*symbols, *load_watchlist_symbols(args.watchlist_file)]

    payload = build_quick_decision(
        symbols,
        source=args.source,
        timeout=args.timeout,
        include_tags=not args.no_tags,
        include_event_factors=not args.no_event_factors,
    )
    if args.watchlist:
        payload["summary"]["watchlist_path"] = str(args.watchlist_file or "data/swing_watchlist_latest.json")
    if not args.no_save:
        payload["summary"]["run_id"] = save_quick_decision(payload)
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_quick_decision(payload)


def _parse_horizons(value: str | None) -> list[int]:
    if not value:
        return [1, 3, 5]
    out: list[int] = []
    for part in str(value).split(","):
        try:
            number = int(part.strip())
        except Exception:
            continue
        if number > 0 and number not in out:
            out.append(number)
    return out or [1, 3, 5]
