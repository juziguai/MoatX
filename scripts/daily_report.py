"""每日调度运行报告

从 warehouse.db 读取当日任务执行记录，生成格式化报告。

用法:
    python scripts/daily_report.py              # 今日报告
    python scripts/daily_report.py --date 2026-04-25   # 指定日期
    python scripts/daily_report.py --json       # 输出 JSON 格式（供飞书推送使用）
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.config import cfg
from modules.db import DatabaseManager


def _connect():
    return DatabaseManager(cfg().data.warehouse_path)


def _query_runs(db: DatabaseManager, target_date: date):
    conn = db.conn
    cursor = conn.cursor()
    cursor.execute(
        """SELECT task_id, task_name, success, duration_ms, error, started_at, finished_at
           FROM task_execution_log
           WHERE date(started_at) = ?
           ORDER BY started_at""",
        (str(target_date),),
    )
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def _fmt_ms(ms: int) -> str:
    if ms < 1000:
        return f"{ms}ms"
    return f"{ms / 1000:.1f}s"


def format_text(runs: list[dict], target_date: date) -> str:
    if not runs:
        return f"[{target_date}] 暂无任务执行记录"

    total = len(runs)
    success = sum(1 for r in runs if r["success"])
    failed = total - success
    rate = success / total * 100 if total else 0
    total_ms = sum(r["duration_ms"] or 0 for r in runs)

    # 按 task_id 分组
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in runs:
        groups[r["task_id"]].append(r)

    lines = [
        f"MoatX 每日运行报告 — {target_date}",
        "=" * 48,
        f"  总任务数: {total}",
        f"  成功: {success}  |  失败: {failed}  |  成功率: {rate:.0f}%",
        f"  总耗时: {_fmt_ms(total_ms)}",
        "",
        "  各任务详情:",
        "  " + "-" * 44,
    ]

    for task_id, grp in sorted(groups.items()):
        name = grp[0]["task_name"]
        ok = sum(1 for r in grp if r["success"])
        ng = len(grp) - ok
        avg_ms = sum(r["duration_ms"] or 0 for r in grp) / len(grp)
        pct = ok / len(grp) * 100
        sym = "✅" if pct >= 90 else ("⚠️" if pct >= 70 else "❌")
        lines.append(
            f"  {sym} {task_id:<20s}  {ok}/{len(grp)}  avg {_fmt_ms(int(avg_ms))}"
        )
        for r in grp:
            if not r["success"]:
                err = (r["error"] or "").strip().split("\n")[0][:60]
                started = r["started_at"] or ""
                lines.append(f"      └─ {started}  {err}")

    lines.append("=" * 48)
    return "\n".join(lines)


def format_json(runs: list[dict], target_date: date) -> str:
    total = len(runs)
    success = sum(1 for r in runs if r["success"])

    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in runs:
        groups[r["task_id"]].append(r)

    tasks = {}
    for task_id, grp in groups.items():
        ok = sum(1 for r in grp if r["success"])
        avg_ms = sum(r["duration_ms"] or 0 for r in grp) / len(grp)
        errors = [r["error"] for r in grp if not r["success"]]
        tasks[task_id] = {
            "name": grp[0]["task_name"],
            "total": len(grp),
            "success": ok,
            "fail": len(grp) - ok,
            "avg_duration_ms": int(avg_ms),
            "success_rate": round(ok / len(grp) * 100, 1),
            "errors": [e.strip().split("\n")[0] for e in errors if e],
        }

    return json.dumps(
        {
            "date": str(target_date),
            "total_runs": total,
            "success_count": success,
            "fail_count": total - success,
            "success_rate": round(success / total * 100, 1) if total else 0,
            "tasks": tasks,
        },
        ensure_ascii=False,
        indent=2,
    )


def main():
    parser = argparse.ArgumentParser(description="MoatX 每日运行报告")
    parser.add_argument("--date", default=None, help="目标日期 (YYYY-MM-DD)，默认今天")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    args = parser.parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"日期格式错误: {args.date}，应为 YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        target_date = date.today()

    db = _connect()
    try:
        runs = _query_runs(db, target_date)
    finally:
        db.close()

    if args.json:
        print(format_json(runs, target_date))
    else:
        print(format_text(runs, target_date))


if __name__ == "__main__":
    main()
