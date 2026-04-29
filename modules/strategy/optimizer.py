"""网格搜索参数优化器。"""

from __future__ import annotations

import importlib
import itertools
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

from modules.backtest.engine import BacktestEngine
from .base import ParametrizedStrategy

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_strategy_params(strategy_name: str, path: Path | None = None) -> dict | None:
    """从 JSON 文件加载指定策略的最优参数。找不到返回 None。"""
    path = path or (_PROJECT_ROOT / "data" / "strategy_params.json")
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        entry = data.get(strategy_name)
        return entry.get("params") if entry else None
    except Exception:
        return None


def save_params_to_json(
    strategy_name: str,
    best_params: dict,
    best_result: dict,
    metric: str,
    best_metric_value: float,
    path: Path | None = None,
) -> Path:
    """将最优参数序列化到 JSON 文件。返回文件路径。"""
    import datetime
    path = path or (_PROJECT_ROOT / "data" / "strategy_params.json")
    data = {
        "strategy": strategy_name,
        "params": best_params,
        "result": best_result,
        "metric": metric,
        "best_metric_value": best_metric_value,
        "saved_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    existing[strategy_name] = data
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _run_single_subprocess(
    strategy_cls_path: str,
    params: dict,
    symbols: list,
    start: str,
    end: str,
    capital: float,
    metric: str,
) -> dict:
    """Run single backtest in a subprocess (used by ProcessPoolExecutor)."""
    mod_path, cls_name = strategy_cls_path.rsplit(".", 1)
    mod = importlib.import_module(mod_path)
    cls = getattr(mod, cls_name)
    strategy = cls()
    strategy.set_params(**params)
    engine = BacktestEngine(
        symbols=symbols,
        start=date.fromisoformat(start),
        end=date.fromisoformat(end),
        initial_capital=capital,
    )
    result = engine.run(strategy)
    return {"params": params, "metric_value": result.get(metric, -999), **result}


class StrategyOptimizer:
    """网格搜索参数优化器。

    对 ParametrizedStrategy 的参数字典空间进行笛卡尔积搜索。
    """

    def __init__(self, db=None):
        self.db = db

    def build_grid(self, strategy_cls: type[ParametrizedStrategy]) -> list[dict[str, Any]]:
        """从 param_specs() 生成所有参数组合。"""
        specs = strategy_cls.param_specs()
        if not specs:
            return [{}]

        grid = []
        keys = []
        for spec in specs:
            keys.append(spec.name)
            if spec.type == "int":
                lo, hi = spec.range or (1, 10)
                step = max(1, (hi - lo) // 5) if spec.range else 2
                grid.append(list(range(lo, hi + 1, step)))
            elif spec.type == "float":
                lo, hi = spec.range or (0.0, 1.0)
                steps = 5
                step_size = (hi - lo) / steps
                grid.append([round(lo + i * step_size, 2) for i in range(steps + 1)])
            elif spec.type == "categorical":
                grid.append(spec.range or [spec.default])
            elif spec.type == "bool":
                grid.append([True, False])

        return [dict(zip(keys, combo)) for combo in itertools.product(*grid)]

    def optimize(
        self,
        strategy_cls: type[ParametrizedStrategy],
        symbols: list[str],
        start: date,
        end: date,
        metric: str = "sharpe_ratio",
        initial_capital: float = 100_000,
        max_workers: int = 4,
    ) -> dict:
        """执行网格搜索，返回最优参数和所有结果。"""
        param_grid = self.build_grid(strategy_cls)
        num_params = len(param_grid)
        start_time = time.time()

        cls_path = f"{strategy_cls.__module__}.{strategy_cls.__qualname__}"
        all_results: list[dict] = []

        if num_params <= 4:
            for params in param_grid:
                result = self._run_single_local(strategy_cls, params, symbols, start, end, initial_capital, metric)
                all_results.append(result)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as ex:
                futures = {
                    ex.submit(
                        _run_single_subprocess, cls_path, params, symbols,
                        start.isoformat(), end.isoformat(), initial_capital, metric,
                    ): params for params in param_grid
                }
                for future in as_completed(futures):
                    try:
                        all_results.append(future.result())
                    except Exception as e:
                        all_results.append({"params": futures[future], "error": str(e), "metric_value": -999})

        all_results.sort(key=lambda r: r.get("metric_value", -999), reverse=True)
        best = all_results[0] if all_results else {}
        elapsed = int((time.time() - start_time) * 1000)

        if self.db is not None:
            try:
                self.db.backtest().save_optimization(
                    strategy_name=strategy_cls.__name__,
                    symbols=symbols,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    target_metric=metric,
                    best_params=best.get("params", {}),
                    best_result={k: v for k, v in best.items() if k != "params"},
                    total_runs=num_params,
                    duration_ms=elapsed,
                )
            except Exception:
                pass

        try:
            save_params_to_json(
                strategy_name=strategy_cls.__name__,
                best_params=best.get("params", {}),
                best_result={k: v for k, v in best.items() if k not in ("params", "metric_value")},
                metric=metric,
                best_metric_value=best.get("metric_value", -999),
            )
        except Exception:
            pass

        return {
            "best_params": best.get("params", {}),
            "best_result": {k: v for k, v in best.items() if k not in ("params", "metric_value")},
            "all_results": all_results,
            "total": num_params,
            "best_metric_value": best.get("metric_value", -999),
            "duration_ms": elapsed,
        }

    def _run_single_local(self, strategy_cls, params, symbols, start, end, capital, metric) -> dict:
        strategy = strategy_cls()
        strategy.set_params(**params)
        engine = BacktestEngine(
            symbol=symbols,
            start=start,
            end=end,
            initial_capital=capital,
        )
        try:
            result = engine.run(strategy)
            return {"params": params, "metric_value": result.get(metric, -999), **result}
        except Exception as e:
            return {"params": params, "error": str(e), "metric_value": -999}
