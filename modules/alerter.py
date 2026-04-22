"""
alerter.py - 预警推送器：CLI + 文件日志 + 飞书
"""

import os
from datetime import datetime


class Alerter:
    """预警推送：CLI + 文件 + 飞书"""

    def __init__(
        self,
        feishu_webhook: str = None,
        feishu_chat_id: str = None,
        feishu_open_id: str = None
    ):
        """
        Args:
            feishu_webhook: 飞书群机器人 Webhook URL（推荐，最简单）
            feishu_chat_id: 飞书群 ID（im/v1/chat API 用）
            feishu_open_id: 飞书用户 open_id（可选，用于私聊）
        """
        self.feishu_webhook = feishu_webhook
        self.feishu_chat_id = feishu_chat_id
        self.feishu_open_id = feishu_open_id

    def send(self, report: str, title: str = "MoatX 持仓预警"):
        """推送预警报告到所有已配置渠道"""
        self._send_cli(report)
        self._send_file(report)
        if self.feishu_webhook:
            self._send_feishu_webhook(report, title)
        elif self.feishu_chat_id or self.feishu_open_id:
            self._send_feishu_api(report)

    def _send_cli(self, report: str):
        print(report)

    def _send_file(self, report: str):
        os.makedirs("data", exist_ok=True)
        with open("data/alerts.log", "a", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"\n{'='*50}\n[{ts}]\n{report}\n")

    def _send_feishu_webhook(self, report: str, title: str):
        """通过飞书群机器人 Webhook 推送（最简方式）"""
        try:
            import json, urllib.request

            payload = {
                "msg_type": "text",
                "content": {"text": f"{title}\n\n{report}"}
            }
            req = urllib.request.Request(
                self.feishu_webhook,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
                if "ok" not in body.lower() and "200" not in str(resp.status):
                    print(f"[Alerter] 飞书 Webhook 推送失败: {body}")
        except Exception as e:
            print(f"[Alerter] 飞书 Webhook 推送异常: {e}")

    def _get_feishu_credentials(self) -> tuple:
        """
        获取飞书凭证，优先级：
        1. 环境变量 FEISHU_APP_ID / FEISHU_APP_SECRET
        2. Claude-Code-Feishu 配置 ~/.claude-code-feishu/config.yaml
        """
        app_id = os.environ.get("FEISHU_APP_ID")
        app_secret = os.environ.get("FEISHU_APP_SECRET")
        if app_id and app_secret:
            return app_id, app_secret

        # 从 Claude-Code-Feishu 配置读取
        try:
            import yaml
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
        except Exception:
            pass

        return None, None

    def _send_feishu_api(self, report: str):
        """
        通过飞书开放平台 API 推送
        凭证读取顺序：环境变量 -> OpenClaw 配置
        """
        app_id, app_secret = self._get_feishu_credentials()
        if not app_id or not app_secret:
            print("[Alerter] 未找到飞书凭证（请设置 LARK_APP_ID/LARK_APP_SECRET 环境变量，或确保 OpenClaw 飞书插件已配置）")
            return

        try:
            import json, urllib.request, urllib.error

            # 获取 tenant_access_token
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
                print(f"[Alerter] 获取 tenant_access_token 失败: {token_data}")
                return

            # 发送消息
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
                    print(f"[Alerter] 飞书消息发送失败: {result}")
        except Exception as e:
            print(f"[Alerter] 飞书 API 推送异常: {e}")

    @staticmethod
    def format_alert_report(alerts: list, holdings_df=None) -> str:
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
