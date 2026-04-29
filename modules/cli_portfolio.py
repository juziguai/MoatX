"""
cli_portfolio.py - 持仓管理 CLI 入口（向后兼容）

实际实现已迁移到 modules/cli/ 包。
"""

# 向后兼容：直接转发到新模块
from modules.cli import main

if __name__ == "__main__":
    main()
