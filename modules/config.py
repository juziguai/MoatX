"""MoatX unified configuration.

Priority (high → low):
  1. Runtime override (config.set())
  2. Environment variable (MOATX_*)
  3. TOML config file
  4. Built-in defaults
"""

from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 compatibility
    import tomli as tomllib

_CONFIG_DIR: Path = Path(__file__).resolve().parent.parent / "data"
_DEFAULT_CONFIG_PATH: Path = _CONFIG_DIR / "moatx.toml"

# Runtime overrides (set by config.set())
_runtime_overrides: dict[str, Any] = {}
_runtime_overrides_lock: threading.RLock = threading.RLock()

# Global singleton cache
_config: MoatXConfig | None = None
_config_lock: threading.RLock = threading.RLock()

# Type alias for the TOML raw dict
_TomlDict = dict[str, Any]


def _env_key(path: str) -> str:
    """Convert nested key to MOATX_ env var name.

    e.g. "crawler.timeout" → "MOATX_CRAWLER_TIMEOUT"
    """
    return "MOATX_" + re.sub(r"[^A-Z0-9_]", "_", path.upper()).strip("_")


def _get_env(path: str) -> str | None:
    return os.environ.get(_env_key(path))


def _deep_get(d: dict[str, Any], path: str) -> Any | None:
    parts = path.split(".")
    for p in parts:
        if isinstance(d, dict):
            d = d.get(p, {})
        else:
            return None
    return d if d != {} else None


@dataclass(frozen=True)
class CacheSettings:
    spot_seconds: int = 30
    board_seconds: int = 300
    concept_board_seconds: int = 300
    fundflow_seconds: int = 600
    f10_seconds: int = 7200
    tencent_quote_seconds: int = 10
    sector_fallback_seconds: int = 300

    def __post_init__(self) -> None:
        for name, val in [("spot_seconds", self.spot_seconds), ("board_seconds", self.board_seconds),
                           ("concept_board_seconds", self.concept_board_seconds), ("fundflow_seconds", self.fundflow_seconds),
                           ("f10_seconds", self.f10_seconds), ("tencent_quote_seconds", self.tencent_quote_seconds),
                           ("sector_fallback_seconds", self.sector_fallback_seconds)]:
            if val < 0:
                raise ValueError(f"CacheSettings.{name} 不能为负数，实际为 {val}")


@dataclass(frozen=True)
class CrawlerSettings:
    timeout: int = 10
    retries: int = 2
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/90.0.4430.85 Safari/537.36"
    )

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError(f"CrawlerSettings.timeout 必须 > 0，实际为 {self.timeout}")
        if self.retries < 0:
            raise ValueError(f"CrawlerSettings.retries 不能为负数，实际为 {self.retries}")


@dataclass(frozen=True)
class DataSourceSettings:
    """实时行情数据源优先级配置。"""

    primary: str = "sina"
    mode: str = "validate"
    validation: tuple[str, ...] | list[str] = field(default_factory=lambda: ("tencent",))
    supplement: tuple[str, ...] | list[str] = field(default_factory=lambda: ("eastmoney",))

    def __post_init__(self) -> None:
        supported = {"sina", "tencent", "eastmoney"}
        supported_modes = {"single", "validate"}
        primary = str(self.primary).strip().lower()
        mode = str(self.mode).strip().lower()
        validation = tuple(str(item).strip().lower() for item in self.validation if str(item).strip())
        supplement = tuple(str(item).strip().lower() for item in self.supplement if str(item).strip())

        if primary not in supported:
            raise ValueError(f"DataSourceSettings.primary unsupported: {self.primary}")
        if mode not in supported_modes:
            raise ValueError(f"DataSourceSettings.mode unsupported: {self.mode}")
        unknown = [item for item in validation + supplement if item not in supported]
        if unknown:
            raise ValueError(f"DataSourceSettings contains unsupported sources: {unknown}")

        object.__setattr__(self, "primary", primary)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "validation", validation)
        object.__setattr__(self, "supplement", supplement)

    def ordered_sources(self, mode: str | None = None) -> list[str]:
        """返回去重后的数据源顺序。

        single: 只返回主源；validate: 主源 -> 校验源 -> 补充源。
        """
        effective_mode = str(mode or self.mode).strip().lower()
        if effective_mode == "single":
            return [self.primary]
        if effective_mode != "validate":
            raise ValueError(f"DataSourceSettings.mode unsupported: {effective_mode}")
        order: list[str] = []
        for item in (self.primary, *self.validation, *self.supplement):
            if item and item not in order:
                order.append(item)
        return order


@dataclass(frozen=True)
class ThreadPoolSettings:
    sina_spot_workers: int = 8
    risk_check_workers: int = 20
    financial_risk_workers: int = 5

    def __post_init__(self) -> None:
        for name, val in [("sina_spot_workers", self.sina_spot_workers),
                           ("risk_check_workers", self.risk_check_workers),
                           ("financial_risk_workers", self.financial_risk_workers)]:
            if val <= 0:
                raise ValueError(f"ThreadPoolSettings.{name} 必须 >= 1，实际为 {val}")


@dataclass(frozen=True)
class AnalysisSettings:
    default_days: int = 120
    adjust: str = "qfq"

    def __post_init__(self) -> None:
        if self.default_days <= 0:
            raise ValueError(f"AnalysisSettings.default_days 必须 > 0，实际为 {self.default_days}")


@dataclass(frozen=True)
class FeeSettings:
    commission_rate: float = 0.0003  # 万3
    stamp_tax_rate: float = 0.001    # 千1
    transfer_fee_rate: float = 0.00001  # 万0.1
    min_commission: float = 5.0

    def __post_init__(self) -> None:
        for name, val in [("commission_rate", self.commission_rate),
                           ("stamp_tax_rate", self.stamp_tax_rate),
                           ("transfer_fee_rate", self.transfer_fee_rate),
                           ("min_commission", self.min_commission)]:
            if val < 0:
                raise ValueError(f"FeeSettings.{name} 不能为负数，实际为 {val}")


@dataclass(frozen=True)
class DataSettings:
    warehouse_path: str = "data/warehouse.db"
    alerts_log_path: str = "data/alerts.log"
    enable_warehouse: bool = True
    save_indicators: bool = False

    def __post_init__(self) -> None:
        pass


@dataclass(frozen=True)
class WebSettings:
    host: str = "127.0.0.1"
    port: int = 8080
    debug: bool = False

    def __post_init__(self) -> None:
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"WebSettings.port 必须在 1-65535 范围内，实际为 {self.port}")


@dataclass(frozen=True)
class FeishuSettings:
    webhook: str = ""
    chat_id: str = ""
    open_id: str = ""

    def __post_init__(self) -> None:
        pass  # 飞书设置暂不校验，允许空值（CLI 模式）


@dataclass(frozen=True)
class AlertSettings:
    """告警相关配置"""
    enabled: bool = True
    cooldown_minutes: int = 5  # 同一标的同一类型告警的冷却时间（分钟）
    max_daily_per_symbol: int = 3  # 单标的每日最大告警次数

    def __post_init__(self) -> None:
        if self.cooldown_minutes < 0:
            raise ValueError(f"AlertSettings.cooldown_minutes 不能为负数，实际为 {self.cooldown_minutes}")
        if self.max_daily_per_symbol <= 0:
            raise ValueError(f"AlertSettings.max_daily_per_symbol 必须 >= 1，实际为 {self.max_daily_per_symbol}")


@dataclass(frozen=True)
class BacktestSettings:
    """回测相关配置"""
    initial_capital: float = 1_000_000.0  # 初始资金
    risk_free_rate: float = 0.03          # 无风险利率（年化）
    benchmark: str = "000300"             # 基准指数（沪深300）
    commission_rate: float = 0.0003       # 佣金费率
    slippage_pct: float = 0.001          # 滑点（千分之一）

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError(f"BacktestSettings.initial_capital 必须 > 0，实际为 {self.initial_capital}")
        if self.risk_free_rate < 0:
            raise ValueError(f"BacktestSettings.risk_free_rate 不能为负数，实际为 {self.risk_free_rate}")


@dataclass(frozen=True)
class SimulationSettings:
    """模拟交易相关配置"""
    # 买入
    max_single_position_pct: float = 0.20
    max_total_position_pct: float = 0.80
    max_buy_count: int = 5                 # 每次最多买入几只
    min_buy_signal_score: int = 3
    pe_max: float = 50.0
    risk_score_max: int = 30
    initial_capital: float = 100_000.0
    # 卖出
    stop_profit_pct: float = 0.15          # 止盈阈值（+15%）
    stop_loss_pct: float = 0.07             # 止损阈值（-7%）
    max_hold_days: int = 20                 # 最大持有天数
    kdj_overbought: float = 85.0            # KDJ 超买阈值
    rsi_overbought: float = 75.0            # RSI 超买阈值

    def __post_init__(self) -> None:
        for name, val in [
            ("max_single_position_pct", self.max_single_position_pct),
            ("max_total_position_pct", self.max_total_position_pct),
            ("stop_profit_pct", self.stop_profit_pct),
            ("stop_loss_pct", self.stop_loss_pct),
        ]:
            if not (0 < val <= 1):
                raise ValueError(f"SimulationSettings.{name} 必须在 (0, 1] 范围内，实际为 {val}")
        if self.max_buy_count <= 0:
            raise ValueError(f"SimulationSettings.max_buy_count 必须 > 0，实际为 {self.max_buy_count}")
        if self.initial_capital <= 0:
            raise ValueError(f"SimulationSettings.initial_capital 必须 > 0，实际为 {self.initial_capital}")
        if self.max_hold_days <= 0:
            raise ValueError(f"SimulationSettings.max_hold_days 必须 > 0，实际为 {self.max_hold_days}")


@dataclass(frozen=True)
class EventIntelligenceSettings:
    """宏观事件情报相关配置"""

    max_news_age_days: int = 14
    notify_probability_threshold: float = 0.55
    notify_opportunity_threshold: float = 75.0
    notify_cooldown_hours: int = 6
    notify_probability_delta: float = 0.10
    monitor_enabled: bool = True
    monitor_top_events: int = 3

    def __post_init__(self) -> None:
        if self.max_news_age_days <= 0:
            raise ValueError(
                f"EventIntelligenceSettings.max_news_age_days 必须 > 0，实际为 {self.max_news_age_days}"
            )
        if not 0 <= self.notify_probability_threshold <= 1:
            raise ValueError("EventIntelligenceSettings.notify_probability_threshold must be between 0 and 1")
        if self.notify_opportunity_threshold < 0:
            raise ValueError("EventIntelligenceSettings.notify_opportunity_threshold must be >= 0")
        if self.notify_cooldown_hours < 0:
            raise ValueError("EventIntelligenceSettings.notify_cooldown_hours must be >= 0")
        if self.notify_probability_delta < 0:
            raise ValueError("EventIntelligenceSettings.notify_probability_delta must be >= 0")
        if self.monitor_top_events <= 0:
            raise ValueError("EventIntelligenceSettings.monitor_top_events must be > 0")


@dataclass(frozen=True)
class RiskControlSettings:
    stop_loss_pct: float = 7.0           # 亏损 N% 触发止损预警
    stop_loss_action: str = "notify"      # notify | auto_sell
    max_single_position_pct: float = 30.0 # 单只股票最大仓位占比（%）
    max_total_position_pct: float = 90.0   # 总仓位上限（%）
    max_daily_loss_pct: float = 5.0      # 单日亏损超过 N% 触发预警
    max_daily_loss_action: str = "notify" # notify | auto_sell
    check_interval_minutes: int = 5       # 检查间隔（分钟，与 scheduler 共用）

    def __post_init__(self) -> None:
        for name, val in [
            ("stop_loss_pct", self.stop_loss_pct),
            ("max_single_position_pct", self.max_single_position_pct),
            ("max_total_position_pct", self.max_total_position_pct),
            ("max_daily_loss_pct", self.max_daily_loss_pct),
        ]:
            if val < 0:
                raise ValueError(f"RiskControlSettings.{name} 不能为负数，实际为 {val}")
        if self.check_interval_minutes <= 0:
            raise ValueError(f"RiskControlSettings.check_interval_minutes 必须 >= 1，实际为 {self.check_interval_minutes}")
        if self.stop_loss_action not in ("notify", "auto_sell"):
            raise ValueError(f"RiskControlSettings.stop_loss_action 必须是 notify 或 auto_sell，实际为 {self.stop_loss_action}")
        if self.max_daily_loss_action not in ("notify", "auto_sell"):
            raise ValueError(f"RiskControlSettings.max_daily_loss_action 必须是 notify 或 auto_sell，实际为 {self.max_daily_loss_action}")


@dataclass(frozen=True)
class MoatXConfig:
    cache: CacheSettings = field(default_factory=CacheSettings)
    crawler: CrawlerSettings = field(default_factory=CrawlerSettings)
    datasource: DataSourceSettings = field(default_factory=DataSourceSettings)
    thread_pool: ThreadPoolSettings = field(default_factory=ThreadPoolSettings)
    analysis: AnalysisSettings = field(default_factory=AnalysisSettings)
    fees: FeeSettings = field(default_factory=FeeSettings)
    data: DataSettings = field(default_factory=DataSettings)
    web: WebSettings = field(default_factory=WebSettings)
    feishu: FeishuSettings = field(default_factory=FeishuSettings)
    risk_control: RiskControlSettings = field(default_factory=RiskControlSettings)
    alert: AlertSettings = field(default_factory=AlertSettings)
    backtest: BacktestSettings = field(default_factory=BacktestSettings)
    simulation: SimulationSettings = field(default_factory=SimulationSettings)
    event_intelligence: EventIntelligenceSettings = field(default_factory=EventIntelligenceSettings)

    def to_dict(self) -> dict[str, Any]:
        d = {}
        for section in (
            "cache",
            "crawler",
            "datasource",
            "thread_pool",
            "analysis",
            "fees",
            "data",
            "web",
            "feishu",
            "risk_control",
            "alert",
            "backtest",
            "simulation",
            "event_intelligence",
        ):
            d[section] = {}
            for k, v in getattr(self, section).__dict__.items():
                d[section][k] = v
        return d


def _load_toml(path: Path | None = None) -> dict[str, Any]:
    path = path or _DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _load_feishu_toml() -> dict[str, Any]:
    """Load feishu config from separate feishu.toml if present."""
    feishu_path = _CONFIG_DIR / "feishu.toml"
    if not feishu_path.exists():
        return {}
    with feishu_path.open("rb") as f:
        return tomllib.load(f)


def _load_simulation_toml() -> dict[str, Any]:
    """Load simulation config from separate simulation.toml if present."""
    sim_path = _CONFIG_DIR / "simulation.toml"
    if not sim_path.exists():
        return {}
    with sim_path.open("rb") as f:
        return tomllib.load(f)


def _apply_env_overrides(raw: dict[str, Any]) -> dict[str, Any]:
    """Apply MOATX_* environment variable overrides in-place."""
    for section, fields in raw.items():
        if not isinstance(fields, dict):
            continue
        for key in list(fields.keys()):
            env_val = _get_env(f"{section}.{key}")
            if env_val is not None:
                try:
                    # preserve type from TOML default
                    orig_type = type(fields[key])
                    if orig_type is bool:
                        fields[key] = env_val.lower() in ("true", "1", "yes")
                    else:
                        fields[key] = orig_type(env_val)
                except (ValueError, TypeError):
                    fields[key] = env_val
    return raw


def _apply_runtime_overrides(raw: dict[str, Any]) -> dict[str, Any]:
    """Apply runtime config.set() overrides."""
    with _runtime_overrides_lock:
        for path, value in _runtime_overrides.items():
            parts = path.split(".")
            target = raw
            for p in parts[:-1]:
                target = target.setdefault(p, {})
            target[parts[-1]] = value
    return raw


def _merge_configs(*dicts: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    """Merge dicts left-to-right (later overrides earlier)."""
    merged = {}
    for d in dicts:
        for k, v in d.items():
            if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
                merged[k] = _merge_configs(merged[k], v)
            else:
                merged[k] = v
    return merged


def set(key: str, value: Any) -> None:
    """Set a runtime configuration override (session-only, not persisted to disk).

    Note: Only keys prefixed with "feishu." are persisted to data/feishu.toml
    when save() is called. All other keys are session-only and lost on restart.

    Args:
        key: Dotted path like "crawler.timeout" or "feishu.webhook"
        value: The value to set
    """
    global _config
    with _runtime_overrides_lock:
        _runtime_overrides[key] = value
    with _config_lock:
        _config = None  # invalidate cache


def save() -> None:
    """把 feishu runtime override 写回 data/feishu.toml（持久化）。

    注意：写入的飞书凭证（webhook/chat_id/open_id）以明文形式保存，
    通过文件系统权限控制（data/ 目录权限）保护。凭证不会出现在 Git 历史或日志中。
    """
    import logging as _logging
    _logger = _logging.getLogger("moatx.config")

    with _runtime_overrides_lock:
        feishu_keys = {k: v for k, v in _runtime_overrides.items() if k.startswith("feishu.")}
    if not feishu_keys:
        return

    feishu_path = _CONFIG_DIR / "feishu.toml"
    # 读取现有值（如果有）
    existing = {}
    if feishu_path.exists():
        with feishu_path.open("rb") as f:
            existing = tomllib.load(f)
    feishu_section = dict(existing.get("feishu", {}))  # 复制，避免修改原 dict

    # 更新 feishu 相关 overrides
    for key, value in feishu_keys.items():
        sub_key = key.split(".", 1)[1]  # "feishu.webhook" -> "webhook"
        feishu_section[sub_key] = value

    existing["feishu"] = feishu_section

    # 手工写 TOML（避免引入 tomli 依赖）
    lines = ["# 飞书配置", "[feishu]", ""]
    for k, v in feishu_section.items():
        lines.append(f'{k} = "{v}"')
    lines.append("")
    feishu_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # 设置文件权限（Windows 下使用隐藏属性实现类似保护）
    try:
        import stat
        feishu_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # owner: read/write only
    except OSError:
        pass  # 非 POSIX 系统忽略

    _logger.info("飞书配置已保存到 %s（凭证已脱敏）", feishu_path)

    # 清除已保存的 overrides
    with _runtime_overrides_lock:
        for k in feishu_keys:
            _runtime_overrides.pop(k, None)
    with _config_lock:
        _config = None


def get_config(path: Path | None = None) -> MoatXConfig:
    """Load and return the merged configuration."""
    defaults = {
        "cache": {
            "spot_seconds": 30,
            "board_seconds": 300,
            "concept_board_seconds": 300,
            "fundflow_seconds": 600,
            "f10_seconds": 7200,
            "tencent_quote_seconds": 10,
            "sector_fallback_seconds": 300,
        },
        "crawler": {
            "timeout": 10,
            "retries": 2,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/90.0.4430.85 Safari/537.36"
            ),
        },
        "thread_pool": {
            "sina_spot_workers": 8,
            "risk_check_workers": 20,
            "financial_risk_workers": 5,
        },
        "datasource": {
            "primary": "sina",
            "mode": "validate",
            "validation": ["tencent"],
            "supplement": ["eastmoney"],
        },
        "analysis": {
            "default_days": 120,
            "adjust": "qfq",
        },
        "fees": {
            "commission_rate": 0.0003,
            "stamp_tax_rate": 0.001,
            "transfer_fee_rate": 0.00001,
            "min_commission": 5.0,
        },
        "feishu": {
            "webhook": "",
            "chat_id": "",
            "open_id": "",
        },
        "risk_control": {
            "stop_loss_pct": 7.0,
            "stop_loss_action": "notify",
            "max_single_position_pct": 30.0,
            "max_total_position_pct": 90.0,
            "max_daily_loss_pct": 5.0,
            "max_daily_loss_action": "notify",
            "check_interval_minutes": 5,
        },
        "alert": {
            "enabled": True,
            "cooldown_minutes": 5,
            "max_daily_per_symbol": 3,
        },
        "backtest": {
            "initial_capital": 1_000_000.0,
            "risk_free_rate": 0.03,
            "benchmark": "000300",
            "commission_rate": 0.0003,
            "slippage_pct": 0.001,
        },
        "simulation": {
            "max_single_position_pct": 0.20,
            "max_total_position_pct": 0.80,
            "min_buy_signal_score": 3,
            "pe_max": 50.0,
            "risk_score_max": 30,
            "initial_capital": 100_000.0,
            "stop_profit_pct": 0.15,
            "stop_loss_pct": 0.07,
            "max_hold_days": 20,
            "kdj_overbought": 85.0,
            "rsi_overbought": 75.0,
        },
        "event_intelligence": {
            "max_news_age_days": 14,
            "notify_probability_threshold": 0.55,
            "notify_opportunity_threshold": 75.0,
            "notify_cooldown_hours": 6,
            "notify_probability_delta": 0.10,
            "monitor_enabled": True,
            "monitor_top_events": 3,
        },
    }

    feishu_raw = _load_feishu_toml()
    sim_raw = _load_simulation_toml()
    raw = _merge_configs(
        defaults,
        _load_toml(path),
        feishu_raw,
        sim_raw,
        _apply_env_overrides(defaults),  # 应用环境变量覆盖
    )
    raw = _apply_runtime_overrides(raw)

    return MoatXConfig(
        cache=CacheSettings(**raw.get("cache", {})),
        crawler=CrawlerSettings(**raw.get("crawler", {})),
        datasource=DataSourceSettings(**raw.get("datasource", {})),
        thread_pool=ThreadPoolSettings(**raw.get("thread_pool", {})),
        analysis=AnalysisSettings(**raw.get("analysis", {})),
        fees=FeeSettings(**raw.get("fees", {})),
        data=DataSettings(**raw.get("data", {})),
        web=WebSettings(**raw.get("web", {})),
        feishu=FeishuSettings(**raw.get("feishu", {})),
        risk_control=RiskControlSettings(**raw.get("risk_control", {})),
        alert=AlertSettings(**raw.get("alert", {})),
        backtest=BacktestSettings(**raw.get("backtest", {})),
        simulation=SimulationSettings(**raw.get("simulation", {})),
        event_intelligence=EventIntelligenceSettings(**raw.get("event_intelligence", {})),
    )


def reload(path: Path | None = None) -> MoatXConfig:
    global _config
    new_config = get_config(path)
    with _config_lock:
        _config = new_config
    return _config


def cfg() -> MoatXConfig:
    global _config
    if _config is None:
        with _config_lock:
            if _config is None:  # 双重检查锁定模式
                _config = get_config()
    return _config


def set_cache_dir(path: str | Path) -> None:
    """设置缓存目录（运行时 override）。"""
    set("data.warehouse_path", str(path))


def close() -> None:
    """关闭配置系统，释放全局单例（测试用）。"""
    global _config
    with _config_lock:
        _config = None
