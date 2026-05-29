"""JSON storage helpers for intraday radar snapshots."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class RadarStorage:
    def __init__(self, base_dir: str | Path = "data/intraday_radar") -> None:
        self.base_dir = Path(base_dir)

    def write_snapshot(self, payload: dict[str, Any], *, prefix: str = "radar") -> Path:
        now = datetime.now()
        date_dir = self.base_dir / now.strftime("%Y%m%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        path = date_dir / f"{prefix}_{now.strftime('%H%M%S')}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        latest = self.base_dir / f"{prefix}_latest.json"
        latest.parent.mkdir(parents=True, exist_ok=True)
        latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
