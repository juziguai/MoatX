"""
scripts/migrate_feishu_to_toml.py

将飞书配置从 portfolio.db config 表迁移到 data/feishu.toml。

迁移后：
  - data/feishu.toml 包含飞书配置
  - portfolio.db config 表中的 feishu_* 条目被清除
  - modules/config.py 优先读 feishu.toml

仅在首次迁移时需要运行。
"""
import os
import sqlite3
import sys
from pathlib import Path

MOATX_DIR = Path(__file__).resolve().parent.parent
FEISHU_TOML = MOATX_DIR / "data" / "feishu.toml"
DB_PATH = MOATX_DIR / "data" / "portfolio.db"


def load_from_db() -> dict:
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.execute(
        "SELECT key, value FROM config WHERE key LIKE 'feishu_%'"
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return {}
    return {row[0]: row[1] for row in rows}


def write_toml(config: dict) -> None:
    FEISHU_TOML.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 飞书配置（由 migrate_feishu_to_toml.py 生成）",
        "# 手动编辑此文件，或使用 python -m modules.cli config --feishu-* 设置",
        "",
        "[feishu]",
    ]
    for key in ("webhook", "chat_id", "open_id"):
        full_key = f"feishu_{key}"
        value = config.get(full_key, "")
        lines.append(f'{key} = "{value}"')
    FEISHU_TOML.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"已写入 {FEISHU_TOML}")


def clear_db(config: dict) -> None:
    if not config:
        return
    conn = sqlite3.connect(str(DB_PATH))
    for key in config:
        conn.execute("DELETE FROM config WHERE key = ?", (key,))
    conn.commit()
    conn.close()
    print(f"已从 portfolio.db 删除 {len(config)} 条飞书配置")


def main() -> None:
    config = load_from_db()
    if not config:
        print("portfolio.db 中没有飞书配置，无需迁移")
        sys.exit(0)

    write_toml(config)
    clear_db(config)
    print("迁移完成")


if __name__ == "__main__":
    main()
