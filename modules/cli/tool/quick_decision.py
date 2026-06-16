"""CLI wrapper for fast intraday buy/no-buy decisions."""

from __future__ import annotations

import json


def cmd_quick_decision(args) -> None:
    from modules.quick_decision import (
        _normalize_sources,
        backfill_quick_decision_samples,
        build_quick_decision,
        evaluate_quick_decisions,
        load_watchlist_symbols,
        learn_quick_decision,
        print_quick_decision,
        print_quick_decision_backfill,
        print_quick_decision_evaluation,
        print_quick_decision_learn,
        print_quick_decision_review,
        print_quick_decision_sample,
        print_quick_decision_summary,
        review_quick_decisions,
        sample_quick_decisions,
        save_quick_decision,
        summarize_quick_decision_evaluations,
    )

    mode = (
        args.symbols[0]
        if args.symbols
        and args.symbols[0]
        in {
            "review",
            "evaluate",
            "summary",
            "dashboard",
            "sample",
            "collect-samples",
            "backfill-samples",
            "replay-samples",
            "learn",
        }
        else ""
    )
    review_mode = args.review or mode == "review"
    evaluate_mode = mode == "evaluate"
    summary_mode = mode in {"summary", "dashboard"}
    review_symbols = args.symbols[1:] if mode else args.symbols
    if mode in {"backfill-samples", "replay-samples"}:
        if not args.start_date:
            raise SystemExit("历史回放需要 --start YYYY-MM-DD")
        payload = backfill_quick_decision_samples(
            review_symbols,
            start_date=args.start_date,
            end_date=args.end_date or args.start_date,
            source=args.replay_source,
            limit=args.limit,
            watchlist_file=args.watchlist_file,
            min_event_score=args.min_event_score,
            include_tags=not args.no_tags,
            include_event_factors=not args.no_event_factors,
            event_factor_max_age_days=args.event_factor_max_age_days,
            save=not args.no_save,
            evaluate_horizons=_parse_horizons(args.horizons) if args.save_evaluation else None,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_backfill(payload)
        return
    if mode in {"sample", "collect-samples"}:
        payload = sample_quick_decisions(
            sources=_normalize_sources(args.sources) if args.sources else None,
            limit=args.limit,
            max_per_symbol_per_day=args.max_per_symbol_per_day,
            source=args.source,
            timeout=args.timeout,
            include_tags=not args.no_tags,
            include_event_factors=not args.no_event_factors,
            watchlist_file=args.watchlist_file,
            min_event_score=args.min_event_score,
            fusion_limit=args.fusion_limit,
            fusion_pool_limit=args.fusion_pool_limit,
            fusion_deadline_seconds=args.fusion_deadline_seconds,
            save=not args.no_save,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_sample(payload)
        return
    if mode == "learn":
        payload = learn_quick_decision(
            horizon_days=args.horizon,
            limit=args.limit,
            min_samples=max(1, int(args.min_samples or 1)),
            suggest_config=args.suggest_config,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_learn(payload)
        return
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
