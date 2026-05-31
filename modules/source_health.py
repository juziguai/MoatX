"""Source health checker - runs probes and logs to warehouse.db.

Called by scheduler before market open and on-demand via CLI.
"""

from __future__ import annotations

import logging

from modules.datasource import QuoteManager, SourceHealth
from modules.db import DatabaseManager

logger = logging.getLogger("moatx.source_health")


def run_health_check(db: DatabaseManager | None = None) -> list[SourceHealth]:
    """Run health check on all configured quote sources and log results.

    Returns list of SourceHealth results.
    """
    mgr = QuoteManager()
    results = mgr.health_check_all()

    if db is None:
        db = DatabaseManager()

    store = db.source_health()
    for r in results:
        store.log(
            source=r.source,
            healthy=r.healthy,
            latency_ms=r.latency_ms,
            error=r.error,
            sample_count=r.sample_count,
        )

    # Check for consecutive failures and alert
    for r in results:
        if not r.healthy:
            consecutive = store.consecutive_failures(r.source)
            if consecutive >= 3:
                logger.warning(
                    "Source %%s has %%d consecutive failures, alert triggered",
                    r.source, consecutive,
                )
                _send_feishu_alert(r.source, consecutive)

    return results


def _send_feishu_alert(source: str, consecutive: int) -> None:
    """Send Feishu alert for consecutive source failures."""
    try:
        from modules.feishu import send_text
        send_text(
            f"[MoatX] 数据源健康告警\n"
            f"数据源: {source}\n"
            f"连续失败次数: {consecutive}\n"
            f"请检查网络或数据源状态"
        )
    except Exception as e:
        logger.warning("Failed to send Feishu alert: %%s", e)


def get_source_status() -> dict[str, dict]:
    """Get latest health status for all sources."""
    db = DatabaseManager()
    store = db.source_health()
    rows = store.latest()
    result = {}
    for row in rows:
        result[row["source"]] = {
            "healthy": bool(row["healthy"]),
            "latency_ms": row["latency_ms"],
            "error": row["error"],
            "checked_at": row["checked_at"],
            "consecutive_failures": store.consecutive_failures(row["source"]),
        }
    return result
