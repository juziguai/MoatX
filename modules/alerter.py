"""
alerter.py - 预警推送器：CLI + 文件日志 + 飞书
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from modules.config import FeishuSettings, cfg

_logger = logging.getLogger("moatx.alerter")

_PUSH_STATS_FILE = Path(__file__).resolve().parent.parent / "data" / "feishu_push_stats.json"
_CONSECUTIVE_FAIL_LIMIT = 3


def _load_push_stats() -> dict:
    if _PUSH_STATS_FILE.exists():
        try:
            return json.loads(_PUSH_STATS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"total": 0, "success": 0, "fail": 0, "consecutive_fails": 0}


def _save_push_stats(stats: dict) -> None:
    _PUSH_STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _PUSH_STATS_FILE.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")


def _record_push(ok: bool) -> None:
    stats = _load_push_stats()
    stats["total"] += 1
    if ok:
        stats["success"] += 1
        stats["consecutive_fails"] = 0
    else:
        stats["fail"] += 1
        stats["consecutive_fails"] += 1
    _save_push_stats(stats)


def get_push_stats() -> dict:
    """Return push statistics."""
    return _load_push_stats()


def _mask(s: str | None, show_tail: int = 4) -> str:
    """返回脱敏后的字符串：仅显示末位，其余用 * 替代。"""
    if not s:
        return "(未设置)"
    if len(s) <= show_tail:
        return "*" * len(s)
    return "*" * (len(s) - show_tail) + s[-show_tail:]


class Alerter:
    """预警推送：CLI + 文件 + 飞书"""

    def __init__(self, feishu_settings: FeishuSettings | None = None) -> None:
        """
        Args:
            feishu_settings: FeishuSettings 实例，包含 webhook/chat_id/open_id
        """
        if feishu_settings is None:
            feishu_settings = cfg().feishu
        self._feishu = feishu_settings

    @property
    def feishu_webhook(self) -> str:
        return self._feishu.webhook

    @property
    def feishu_chat_id(self) -> str:
        return self._feishu.chat_id

    @property
    def feishu_open_id(self) -> str:
        return self._feishu.open_id

    def send(self, report: str, title: str | None = None) -> bool:
        """推送预警报告到所有已配置渠道。返回是否所有渠道均成功（CLI 始终成功）。"""
        if title is None:
            title = "MoatX 持仓预警"
        ok = True
        self._send_cli(report)
        ok = ok and self._send_file(report)
        feishu_ok = True
        if self.feishu_webhook:
            feishu_ok = self._send_feishu_webhook(report, title)
        elif self.feishu_chat_id or self.feishu_open_id:
            feishu_ok = self._send_feishu_api(report)
        _record_push(feishu_ok)
        ok = ok and feishu_ok
        return ok

    def _send_cli(self, report: str) -> None:
        print(report)

    def _send_file(self, report: str) -> bool:
        log_path = Path(cfg().data.alerts_log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"\n{'='*50}\n[{ts}]\n{report}\n")
            return True
        except OSError:
            return False

    def _write_fallback_file(self, report: str, title: str) -> None:
        """Write to fallback file after consecutive Feishu push failures."""
        stats = _load_push_stats()
        if stats.get("consecutive_fails", 0) >= _CONSECUTIVE_FAIL_LIMIT:
            fallback = Path(__file__).resolve().parent.parent / "data" / "feishu_fallback.txt"
            fallback.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(fallback, "a", encoding="utf-8") as f:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"\n[fallback {ts}] {title}\n{report}\n")
            except OSError:
                pass

    def _send_feishu_webhook(self, report: str, title: str) -> bool:
        """通过飞书群机器人 Webhook 推送（富文本卡片），失败重试1次，仍失败写兜底文件。"""
        try:
            import json
            import urllib.request

            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": title},
                        "template": "red" if "CRITICAL" in title or "止损" in report else "orange",
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": report.replace("\n", "\n\n"),
                            }
                        },
                        {"tag": "hr"},
                        {
                            "tag": "note",
                            "elements": [
                                {"tag": "plain_text", "content": f"MoatX 风控 · {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
                            ]
                        }
                    ]
                }
            }
            last_err = None
            for attempt in range(2):
                try:
                    req = urllib.request.Request(
                        self.feishu_webhook,
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST"
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        body = resp.read().decode("utf-8")
                        if "ok" not in body.lower() and "200" not in str(resp.status):
                            last_err = "bad response"
                            if attempt == 0:
                                time.sleep(5)
                                continue
                            break
                    return True
                except Exception as e:
                    last_err = e
                    if attempt == 0:
                        time.sleep(5)
                        continue
                    break
            _logger.warning("飞书 Webhook 推送失败（已重试）: %s", last_err)
            self._write_fallback_file(report, title)
            return False
        except Exception as e:
            _logger.warning("飞书 Webhook 推送异常: %s", e)
            self._write_fallback_file(report, title)
            return False

    def _get_feishu_credentials(self) -> tuple:
        """
        获取飞书凭证，优先级：
        1. 环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET
        2. Claude-Code-Feishu 配置 ~/.claude-code-feishu/config.yaml
        """
        app_id = os.environ.get("FEISHU_APP_ID") or os.environ.get("LARK_APP_ID")
        app_secret = os.environ.get("FEISHU_APP_SECRET") or os.environ.get("LARK_APP_SECRET")
        if app_id and app_secret:
            return app_id, app_secret

        # 从 Claude-Code-Feishu 配置读取
        try:
            import yaml
            for config_path in self._feishu_config_paths():
                if not config_path.exists():
                    continue
                with config_path.open("r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
                feishu = cfg.get("feishu", {})
                app_id = feishu.get("app_id") or feishu.get("appId")
                app_secret = feishu.get("app_secret") or feishu.get("appSecret")
                if app_id and app_secret:
                    return app_id, app_secret
            config_path = os.path.expanduser("~/.claude-code-feishu/config.yaml")
            if not os.path.exists(config_path):
                # 尝试旧路径
                config_path = os.path.join(os.path.dirname(__file__), "..", "..", "Claude-Code-Feishu", "config.yaml")
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            feishu = cfg.get("feishu", {})
            app_id = feishu.get("app_id")
            app_secret = feishu.get("app_secret")
            if app_id and app_secret:
                return app_id, app_secret
        except Exception as e:
            _logger.warning("加载飞书配置文件失败（路径已脱敏）: %s", e)

        return None, None

    @staticmethod
    def _feishu_config_paths() -> list:
        paths = [Path.home() / ".claude-code-feishu" / "config.yaml"]
        userprofile = os.environ.get("USERPROFILE")
        if userprofile:
            paths.append(Path(userprofile) / "Claude-Code-Feishu" / "config.yaml")
        paths.append(Path(__file__).resolve().parents[2] / "Claude-Code-Feishu" / "config.yaml")
        return paths

    def _send_feishu_api(self, report: str) -> bool:
        """
        通过飞书开放平台 API 推送，失败重试1次，仍失败写兜底文件。
        凭证读取顺序：环境变量 -> OpenClaw 配置
        """
        app_id, app_secret = self._get_feishu_credentials()
        if not app_id or not app_secret:
            _logger.warning("飞书凭证缺失，请设置环境变量 FEISHU_APP_ID/FEISHU_APP_SECRET 或 LARK_APP_ID/LARK_APP_SECRET")
            return False

        last_err = None
        for attempt in range(2):
            try:
                import json
                import urllib.request
                import urllib.error

                token_req = urllib.request.Request(
                    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                    data=json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(token_req, timeout=10) as resp:
                    token_data = json.loads(resp.read().decode("utf-8"))
                token = token_data.get("tenant_access_token")
                if not token:
                    last_err = "no token"
                    if attempt == 0:
                        time.sleep(5)
                        continue
                    break

                receive_id = self.feishu_open_id or self.feishu_chat_id
                receive_id_type = "open_id" if self.feishu_open_id else "chat_id"

                msg_req = urllib.request.Request(
                    "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=" + receive_id_type,
                    data=json.dumps({
                        "receive_id": receive_id,
                        "msg_type": "text",
                        "content": json.dumps({"text": report})
                    }).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {token}"
                    },
                    method="POST"
                )
                with urllib.request.urlopen(msg_req, timeout=10) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                    if result.get("code") != 0:
                        last_err = result
                        if attempt == 0:
                            time.sleep(5)
                            continue
                        break
                return True
            except Exception as e:
                last_err = e
                if attempt == 0:
                    time.sleep(5)
                    continue
                break

        _logger.warning("飞书 API 推送失败（已重试）: %s", last_err)
        self._write_fallback_file(report, "飞书推送")
        return False

    def send_signal(self, signal: Any, title: str = "MoatX 交易信号") -> bool:
        """推送结构化信号到所有渠道。"""
        report = self._format_signal_report(signal)
        ok = True
        self._send_cli(report)
        ok = ok and self._send_file(report)
        if self.feishu_webhook:
            ok = ok and self._send_feishu_structured(signal, title)
        return ok

    def _send_feishu_structured(self, signal: Any, title: str) -> bool:
        """发送结构化飞书消息（带信号详情）。"""
        try:
            import json
            import urllib.request
            lines = [
                f"## {title}",
                f"**股票**: {signal.symbol}",
                f"**信号**: {signal.signal_type.upper()}",
                f"**价格**: {signal.price:.2f}",
                f"**置信度**: {signal.confidence:.0f}/100",
                f"**策略**: {signal.strategy_name}",
                f"**原因**: {signal.reason}",
                f"**时间**: {signal.created_at}",
            ]
            if signal.indicators:
                ind = "; ".join(f"{k}={v}" for k, v in signal.indicators.items())
                lines.append(f"**指标**: {ind}")
            text = "\n".join(lines)
            payload = {"msg_type": "text", "content": {"text": text}}
            req = urllib.request.Request(
                self.feishu_webhook,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                if "ok" not in body.lower() and "200" not in str(resp.status):
                    _logger.warning("信号飞书推送失败（webhook 已脱敏）")
                    return False
            return True
        except Exception as e:
            _logger.warning("信号飞书推送异常（webhook 已脱敏）")
            return False

    @staticmethod
    def _format_signal_report(signal) -> str:
        return (
            f"## {signal.signal_type.upper()} {signal.symbol}\n"
            f"- 价格: {signal.price:.2f}\n"
            f"- 策略: {signal.strategy_name}\n"
            f"- 置信度: {signal.confidence:.0f}/100\n"
            f"- 原因: {signal.reason}\n"
            f"- 时间: {signal.created_at}\n"
        )

    def format_feishu_message(self, report: str, title: str | None) -> dict[str, str]:
        """生成飞书消息卡片格式（webhook 用）。"""
        if title is None:
            title = "MoatX 持仓预警"
        return {"msg_type": "text", "content": {"text": f"{title}\n\n{report}"}}

    def _get_token(self) -> str | None:
        """获取 Feishu tenant_access_token。"""
        app_id, app_secret = self._get_feishu_credentials()
        if not app_id or not app_secret:
            return None
        try:
            import json
            import urllib.request
            req = urllib.request.Request(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                data=json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                token_data = json.loads(resp.read().decode("utf-8"))
            return token_data.get("tenant_access_token")
        except Exception:
            return None

    @staticmethod
    def format_alert_report(alerts: list[dict[str, Any]], holdings_df=None) -> str:
        """生成预警 Markdown 报告"""
        if not alerts:
            return "✅ 持仓检查完成，暂无预警"

        lines = ["# MoatX 持仓预警报告", ""]
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"**时间**: {ts}")
        lines.append(f"**预警数量**: {len(alerts)}")
        lines.append("")

        # 按类型分组
        by_type = {}
        for a in alerts:
            by_type.setdefault(a.get("type", "unknown"), []).append(a)

        for alert_type, items in by_type.items():
            lines.append(f"## {items[0]['msg'].split('，')[0]}  ({len(items)} 只)")
            for it in items:
                symbol = it.get("symbol", "")
                lines.append(f"- **{symbol}**: {it.get('msg', '')}")
            lines.append("")

        return "\n".join(lines)
