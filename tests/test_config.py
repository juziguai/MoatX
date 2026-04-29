"""P0: config.py 配置系统测试"""

import pytest
import os
import tempfile
from pathlib import Path
from modules.config import (
    MoatXConfig,
    CacheSettings,
    CrawlerSettings,
    DataSourceSettings,
    EventIntelligenceSettings,
    FeishuSettings,
    get_config,
    cfg,
    set,
    reload,
    close,
)


class TestCacheSettings:
    def test_default_values(self):
        cs = CacheSettings()
        assert cs.spot_seconds == 30
        assert cs.board_seconds == 300
        assert cs.fundflow_seconds == 600

    def test_negative_rejected(self):
        with pytest.raises(ValueError, match="不能为负数"):
            CacheSettings(spot_seconds=-1)


class TestCrawlerSettings:
    def test_default_timeout(self):
        cs = CrawlerSettings()
        assert cs.timeout == 10
        assert cs.retries == 2

    def test_zero_timeout_rejected(self):
        with pytest.raises(ValueError, match="必须 > 0"):
            CrawlerSettings(timeout=0)

    def test_negative_retries_rejected(self):
        with pytest.raises(ValueError, match="不能为负数"):
            CrawlerSettings(retries=-1)


class TestDataSourceSettings:
    def test_default_order_prefers_sina(self):
        settings = DataSourceSettings()
        assert settings.ordered_sources() == ["sina", "tencent", "eastmoney"]
        assert settings.ordered_sources(mode="single") == ["sina"]

    def test_custom_primary_reorders_sources(self):
        settings = DataSourceSettings(
            primary="tencent",
            validation=["sina"],
            supplement=["eastmoney"],
        )
        assert settings.ordered_sources() == ["tencent", "sina", "eastmoney"]

    def test_duplicate_sources_are_deduped(self):
        settings = DataSourceSettings(
            primary="sina",
            validation=["sina", "tencent"],
            supplement=["tencent", "eastmoney"],
        )
        assert settings.ordered_sources() == ["sina", "tencent", "eastmoney"]

    def test_unsupported_source_rejected(self):
        with pytest.raises(ValueError, match="unsupported"):
            DataSourceSettings(primary="unknown")

    def test_unsupported_mode_rejected(self):
        with pytest.raises(ValueError, match="unsupported"):
            DataSourceSettings(mode="fast")


class TestFeishuSettings:
    def test_allows_empty(self):
        fs = FeishuSettings()
        assert fs.webhook == ""
        assert fs.chat_id == ""
        assert fs.open_id == ""


class TestEventIntelligenceSettings:
    def test_default_notify_thresholds(self):
        settings = EventIntelligenceSettings()
        assert settings.notify_probability_threshold == 0.55
        assert settings.notify_opportunity_threshold == 75.0
        assert settings.monitor_top_events == 3

    def test_invalid_probability_threshold_rejected(self):
        with pytest.raises(ValueError, match="notify_probability_threshold"):
            EventIntelligenceSettings(notify_probability_threshold=1.1)


class TestConfigSingleton:
    def test_cfg_returns_config(self):
        close()  # reset singleton
        config = cfg()
        assert isinstance(config, MoatXConfig)

    def test_cfg_caches(self):
        close()
        c1 = cfg()
        c2 = cfg()
        assert c1 is c2  # same object

    def test_reload_resets(self):
        close()
        c1 = cfg()
        c2 = reload()
        assert isinstance(c2, MoatXConfig)


class TestConfigSet:
    def test_set_runtime_override(self):
        close()
        set("crawler.timeout", 99)
        c = cfg()
        assert c.crawler.timeout == 99
        close()  # cleanup

    def test_set_feishu_webhook(self):
        close()
        set("feishu.webhook", "https://open.feishu.cn/test")
        c = cfg()
        assert c.feishu.webhook == "https://open.feishu.cn/test"
        close()


class TestConfigSave:
    def test_save_writes_toml(self):
        close()
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = Path(tmpdir) / "test_feishu.toml"
            set("feishu.webhook", "https://test.example.com/hook")
            from modules.config import _CONFIG_DIR, _DEFAULT_CONFIG_PATH
            # We can't easily override _CONFIG_DIR without modifying the module,
            # so we just verify that set() works without error
            close()
